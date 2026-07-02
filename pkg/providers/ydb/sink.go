package ydb

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strings"
	"sync"
	text_template "text/template"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dustin/go-humanize"
	"github.com/google/uuid"
	"github.com/transferia/transferia/internal/logger"
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	ydb_decimal "github.com/transferia/transferia/pkg/providers/ydb/decimal"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/xtls"
	ydb_go_sdk "github.com/ydb-platform/ydb-go-sdk/v3"
	ydb_credentials "github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	ydb_scheme "github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	ydb_table "github.com/ydb-platform/ydb-go-sdk/v3/table"
	ydb_table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/crc64"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yson"
)

type TemplateModel struct {
	Cols []TemplateCol
	Path string
}

const (
	writeBatchMaxLen  = 10000
	writeBatchMaxSize = 48 * humanize.MiByte // NOTE: RPC message limit for YDB upsert is 64 MB.

	NoCompression = "off"
)

var rowTooLargeRegexp = regexp.MustCompile(`Row cell size of [0-9]+ bytes is larger than the allowed threshold [0-9]+`)

type TemplateCol struct{ Name, Typ, Optional, Comma string }

var insertTemplate, _ = text_template.New("query").Parse(`
{{- /*gotype: TemplateModel*/ -}}
--!syntax_v1
DECLARE $batch AS List<
	Struct<{{ range .Cols }}
		` + "`{{ .Name }}`" + `:{{ .Typ }}{{ .Optional }}{{ .Comma }}{{ end }}
	>
>;
UPSERT INTO ` + "`{{ .Path }}`" + ` ({{ range .Cols }}
		` + "`{{ .Name }}`" + `{{ .Comma }}{{ end }}
)
SELECT{{ range .Cols }}
	` + "`{{ .Name }}`" + `{{ .Comma }}{{ end }}
FROM AS_TABLE($batch)
`)

var deleteTemplate, _ = text_template.New("query").Parse(`
{{- /*gotype: TemplateModel*/ -}}
--!syntax_v1
DECLARE $batch AS Struct<{{ range .Cols }}
	` + "`{{ .Name }}`" + `:{{ .Typ }}{{ .Optional }}{{ .Comma }}{{ end }}
>;
DELETE FROM ` + "`{{ .Path }}`" + `
WHERE 1=1
{{ range .Cols }}
	and ` + "`{{ .Name }}`" + ` = $batch.` + "`{{ .Name }}`" + `{{ end }}
`)

var createTableQueryTemplate, _ = text_template.New(
	"createTableQuery",
).Funcs(
	text_template.FuncMap{
		"join": strings.Join,
	},
).Parse(`
{{- /* gotype: TemplateTable */ -}}
--!syntax_v1
CREATE TABLE ` + "`{{ .Path }}`" + ` (
	{{- range .Columns }}
	` + "`{{ .Name }}`" + ` {{ .Type }} {{ if .NotNull }} NOT NULL {{ end }}, {{ end }}
		PRIMARY KEY (` + "`{{ join .Keys \"`, `\" }}`" + `){{ if not .IsTableColumnOriented }},
	FAMILY default (
		COMPRESSION = ` + `"{{ .DefaultCompression }}"` + `
	){{ end }}
)

{{- if .IsTableColumnOriented }}
PARTITION BY HASH(` + "`{{ join .Keys \"`, `\" }}`" + `)
{{- end}}

WITH (
	{{- if .IsTableColumnOriented }}
		STORE = COLUMN
		{{- if gt .ShardCount 0 }}
		, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {{ .ShardCount }}
		{{- end }}
	{{- else }}
		{{- if gt .ShardCount 0 }}
		UNIFORM_PARTITIONS = {{ .ShardCount }}
		{{- else }}
		AUTO_PARTITIONING_BY_SIZE = ENABLED
		{{- end }}
	{{- end }}
);
`)

type ColumnTemplate struct {
	Name string
	Type string
	// For now is supported only for primary keys in OLAP tables
	NotNull bool
}

var TypeYdbDecimal ydb_table_types.Type = ydb_table_types.DecimalType(22, 9)

type AllowedIn string

const (
	BOTH AllowedIn = "both"
	OLTP AllowedIn = "oltp"
	OLAP AllowedIn = "olap"
)

// based on
// https://ydb.tech/ru/docs/yql/reference/types/primitive
// https://ydb.tech/ru/docs/concepts/column-table#olap-data-types
// unmentioned types can't be primary keys
var primaryIsAllowedFor = map[ydb_table_types.Type]AllowedIn{
	// we cast bool to uint8 for OLAP tables
	ydb_table_types.TypeBool: BOTH,
	// we cast int8/16 to int 32 for OLAP tables
	ydb_table_types.TypeInt8:  BOTH,
	ydb_table_types.TypeInt16: BOTH,
	ydb_table_types.TypeInt32: BOTH,
	ydb_table_types.TypeInt64: BOTH,

	ydb_table_types.TypeUint8:  BOTH,
	ydb_table_types.TypeUint16: BOTH,
	ydb_table_types.TypeUint32: BOTH,
	ydb_table_types.TypeUint64: BOTH,

	// we cast dynumber/decimal to string for OLAP tables
	ydb_table_types.TypeDyNumber: BOTH,
	TypeYdbDecimal:               OLAP,

	ydb_table_types.TypeDate:      BOTH,
	ydb_table_types.TypeDatetime:  BOTH,
	ydb_table_types.TypeTimestamp: BOTH,

	ydb_table_types.TypeString: BOTH,
	ydb_table_types.TypeUTF8:   BOTH,
	ydb_table_types.TypeUUID:   OLTP,
	// we cast interval to int64 for OLAP tables
	ydb_table_types.TypeInterval: BOTH,

	ydb_table_types.TypeTzDate:      OLTP,
	ydb_table_types.TypeTzDatetime:  OLTP,
	ydb_table_types.TypeTzTimestamp: OLTP,
}

type CreateTableTemplate struct {
	Path                  string
	Columns               []ColumnTemplate
	Keys                  []string
	ShardCount            int64
	IsTableColumnOriented bool
	DefaultCompression    string
}

var alterColumnCompressionQueryTemplate, _ = text_template.New(
	"alterColumnCompressionQuery",
).Parse(`
{{- /* gotype: AlterColumnCompressionTemplate */ -}}
--!syntax_v1
ALTER TABLE ` + "`{{ .Path }}`" + `
	ALTER COLUMN ` + "`{{ .ColumnName }}`" + ` SET COMPRESSION(algorithm={{ .Compression }});
`)

type AlterColumnCompressionTemplate struct {
	Path        string
	ColumnName  string
	Compression string
}

var alterTableQueryTemplate, _ = text_template.New(
	"alterTableQuery",
).Parse(`
{{- /* gotype: AlterTableTemplate */ -}}
--!syntax_v1
ALTER TABLE ` + "`{{ .Path }}`" + `
{{- range $index, $element := .AddColumns }}
	{{ if ne $index 0 }},{{end}} ADD COLUMN ` + "`{{ $element.Name }}`" + ` {{ $element.Type }} {{ end }}
{{- range $index, $element := .DropColumns }}
{{ if ne $index 0 }},{{end}} DROP COLUMN ` + "`{{ $element }}`" + `{{ end }}
;`)

type AlterTableTemplate struct {
	Path        string
	AddColumns  []ColumnTemplate
	DropColumns []string
}

var dropTableQueryTemplate, _ = text_template.New(
	"dropTableQuery",
).Parse(`
{{- /* gotype: DropTableTemplate */ -}}
--!syntax_v1
DROP TABLE ` + "`{{ .Path }}`" + `;
`)

type DropTableTemplate struct {
	Path string
}

var SchemaMismatchErr = xerrors.New("table deleted, due schema mismatch")

type ydbPath string // without database

func (t *ydbPath) MakeChildPath(child string) ydbPath {
	return ydbPath(path.Join(string(*t), child))
}

type sinker struct {
	config  *YdbDestination
	logger  log.Logger
	metrics *stats.SinkerStats
	locks   sync.Mutex
	lock    sync.Mutex
	cache   map[ydbPath]*abstract.TableSchema
	once    sync.Once
	closeCh chan struct{}
	db      *ydb_go_sdk.Driver
}

func (s *sinker) getRootPath() string {
	rootPath := s.config.Database
	if s.config.Path != "" {
		rootPath = path.Join(s.config.Database, s.config.Path)
	}
	return rootPath
}

func (s *sinker) getTableFullPath(tableName string) ydbPath {
	return ydbPath(path.Join(s.getRootPath(), tableName))
}

func (s *sinker) getFullPath(tablePath ydbPath) string {
	return path.Join(s.db.Name(), string(tablePath))
}

func (s *sinker) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var closeErrs []error
	if err := s.db.Close(ctx); err != nil {
		closeErrs = append(closeErrs, xerrors.Errorf("failed to close a connection to YDB: %w", err))
	}
	s.once.Do(func() {
		close(s.closeCh)
	})
	return errors.Join(closeErrs...)
}

func (s *sinker) isClosed() bool {
	select {
	case <-s.closeCh:
		return true
	default:
		return false
	}
}

func (s *sinker) tryApplyOlapColumnCompression(ctx context.Context, session ydb_table.Session, tablePath string, columns []ColumnTemplate) error {
	if !s.config.IsTableColumnOriented || s.config.DefaultCompression == "" {
		return nil
	}

	for _, column := range columns {
		alterCompression := AlterColumnCompressionTemplate{
			Path:        tablePath,
			ColumnName:  column.Name,
			Compression: s.config.DefaultCompression,
		}
		var query strings.Builder
		if err := alterColumnCompressionQueryTemplate.Execute(&query, alterCompression); err != nil {
			return xerrors.Errorf("unable to execute alter column compression template for column %s: %w", column.Name, err)
		}

		s.logger.Info(
			"Try to alter column compression",
			log.String("table", tablePath),
			log.String("column", column.Name),
			log.String("query", query.String()),
		)
		if err := session.ExecuteSchemeQuery(ctx, query.String()); err != nil {
			return xerrors.Errorf("unable to alter column compression for column %s: %w", column.Name, err)
		}
	}
	return nil
}

func (s *sinker) checkTable(tablePath ydbPath, schema *abstract.TableSchema) error {
	if s.config.IsSchemaMigrationDisabled {
		return nil
	}
	if existingSchema, ok := s.cache[tablePath]; ok && existingSchema.Equal(schema) {
		return nil
	}
	s.locks.Lock()
	defer s.locks.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	exist, err := sugar.IsEntryExists(ctx, s.db.Scheme(), s.getFullPath(tablePath), ydb_scheme.EntryTable, ydb_scheme.EntryColumnTable)
	if err != nil {
		s.logger.Warnf("unable to check existence of table %s: %s", tablePath, err.Error())
	} else {
		s.logger.Infof("check exist %v:%v ", tablePath, exist)
	}
	if !exist {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		nestedPath := strings.Split(string(tablePath), "/")
		for i := range nestedPath[:len(nestedPath)-1] {
			if nestedPath[i] == "" {
				continue
			}
			p := []string{s.config.Database}
			p = append(p, nestedPath[:i+1]...)
			folderPath := path.Join(p...)
			if err := s.db.Scheme().MakeDirectory(ctx, folderPath); err != nil {
				return xerrors.Errorf("unable to make directory: %w", err)
			}
		}
		if err := s.db.Table().Do(ctx, func(ctx context.Context, session ydb_table.Session) error {
			columns := make([]ColumnTemplate, 0)
			keys := make([]string, 0)
			for _, col := range schema.Columns() {
				if col.ColumnName == "_shard_key" {
					continue
				}

				ydbType := s.ydbType(col.DataType, col.OriginalType)
				if ydbType == ydb_table_types.TypeUnknown {
					return abstract.NewFatalError(xerrors.Errorf("YDB create table type %v not supported", col.DataType))
				}

				isPrimaryKey, err := s.isPrimaryKey(ydbType, col)
				if err != nil {
					return abstract.NewFatalError(xerrors.Errorf("Unable to create primary key: %w", err))
				}
				s.logger.Infof("col: %v type: %v isPrimary: %v)", col.ColumnName, ydbType, isPrimaryKey)

				columns = append(columns, ColumnTemplate{
					col.ColumnName,
					ydbType.Yql(),
					isPrimaryKey && s.config.IsTableColumnOriented,
				})

				if isPrimaryKey {
					keys = append(keys, col.ColumnName)
				}
			}

			if s.config.ShardCount > 0 {
				columns = append(columns, ColumnTemplate{"_shard_key", ydb_table_types.TypeUint64.Yql(), s.config.IsTableColumnOriented})

				keys = append([]string{"_shard_key"}, keys...)

				s.logger.Infof("Keys %v", keys)
			}

			currTable := CreateTableTemplate{
				Path:                  s.getFullPath(tablePath),
				Columns:               columns,
				Keys:                  keys,
				ShardCount:            s.config.ShardCount,
				IsTableColumnOriented: s.config.IsTableColumnOriented,
				DefaultCompression:    s.config.DefaultCompression,
			}

			var query strings.Builder
			if err := createTableQueryTemplate.Execute(&query, currTable); err != nil {
				return xerrors.Errorf("unable to execute create table template: %w", err)
			}

			s.logger.Info("Try to create table", log.String("table", s.getFullPath(tablePath)), log.String("query", query.String()))
			if err := session.ExecuteSchemeQuery(ctx, query.String()); err != nil {
				return err
			}
			if err := s.tryApplyOlapColumnCompression(ctx, session, s.getFullPath(tablePath), columns); err != nil {
				return xerrors.Errorf("unable to apply column compression: %w", err)
			}
			return nil
		}); err != nil {
			return xerrors.Errorf("unable to create table: %s: %w", s.getFullPath(tablePath), err)
		}
	} else {
		if err := s.db.Table().Do(context.Background(), func(ctx context.Context, session ydb_table.Session) error {
			describeTableCtx, cancelDescribeTableCtx := context.WithTimeout(ctx, time.Minute)
			defer cancelDescribeTableCtx()
			desc, err := session.DescribeTable(describeTableCtx, s.getFullPath(tablePath))
			if err != nil {
				return xerrors.Errorf("unable to describe path %s: %w", s.getFullPath(tablePath), err)
			}
			s.logger.Infof("check migration %v -> %v", len(desc.Columns), len(schema.Columns()))

			addColumns := make([]ColumnTemplate, 0)
			for _, a := range schema.Columns() {
				exist := false
				for _, b := range FromYdbSchema(desc.Columns, desc.PrimaryKey) {
					if a.ColumnName == b.ColumnName {
						exist = true
					}
				}
				if !exist {
					s.logger.Warnf("add column %v:%v", a.ColumnName, a.DataType)
					addColumns = append(addColumns, ColumnTemplate{
						a.ColumnName,
						s.ydbType(a.DataType, a.OriginalType).Yql(),
						false,
					})
				}
			}

			dropColumns := make([]string, 0)
			if s.config.DropUnknownColumns {
				for _, a := range FromYdbSchema(desc.Columns, desc.PrimaryKey) {
					if a.ColumnName == "_shard_key" && s.config.ShardCount > 0 {
						continue
					}
					exist := false
					for _, b := range schema.Columns() {
						if a.ColumnName == b.ColumnName {
							exist = true
						}
					}
					if !exist {
						s.logger.Warnf("drop column %v:%v", a.ColumnName, a.DataType)
						dropColumns = append(dropColumns, a.ColumnName)
					}
				}
			}

			if len(addColumns) == 0 && len(dropColumns) == 0 {
				return nil
			}

			alterTable := AlterTableTemplate{
				Path:        s.getFullPath(tablePath),
				AddColumns:  addColumns,
				DropColumns: dropColumns,
			}
			var query strings.Builder
			if err := alterTableQueryTemplate.Execute(&query, alterTable); err != nil {
				return xerrors.Errorf("unable to execute alter table template: %w", err)
			}

			alterTableCtx, cancelAlterTableCtx := context.WithTimeout(context.Background(), time.Minute)
			defer cancelAlterTableCtx()
			s.logger.Infof("alter table query:\n %v", query.String())
			if err := session.ExecuteSchemeQuery(alterTableCtx, query.String()); err != nil {
				return err
			}
			if err := s.tryApplyOlapColumnCompression(alterTableCtx, session, s.getFullPath(tablePath), addColumns); err != nil {
				return xerrors.Errorf("unable to apply column compression: %w", err)
			}
			return nil
		}); err != nil {
			s.logger.Warn("unable to apply migration", log.Error(err))
			return xerrors.Errorf("unable to apply migration: %w", err)
		}
	}

	s.lock.Lock()
	defer s.lock.Unlock()
	s.cache[tablePath] = schema
	return nil
}

func (s *sinker) rotateTable() error {
	rootPath := s.getRootPath()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	rootDir, err := s.db.Scheme().ListDirectory(ctx, rootPath)
	if err != nil {
		return xerrors.Errorf("Cannot list directory %s: %w", rootPath, err)
	}
	baseTime := s.config.Rotation.BaseTime()
	s.logger.Infof("Begin rotate table process on %s at %v", rootPath, baseTime)
	s.recursiveCleanupOldTables(ydbPath(s.config.Path), rootDir, baseTime)
	return nil
}

func (s *sinker) recursiveCleanupOldTables(currPath ydbPath, dir ydb_scheme.Directory, baseTime time.Time) {
	for _, child := range dir.Children {
		if child.Name == ".sys_health" || child.Name == ".sys" {
			continue
		}
		switch child.Type {
		case ydb_scheme.EntryDirectory:
			dirPath := path.Join(s.config.Database, string(currPath), child.Name)
			d, err := func() (ydb_scheme.Directory, error) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				return s.db.Scheme().ListDirectory(ctx, dirPath)
			}()
			if err != nil {
				s.logger.Warnf("Unable to list directory %s: %v", dirPath, err)
				continue
			}
			s.recursiveCleanupOldTables(currPath.MakeChildPath(child.Name), d, baseTime)
		case ydb_scheme.EntryTable, ydb_scheme.EntryColumnTable:
			var tableTime time.Time
			switch s.config.Rotation.PartType {
			case model.RotatorPartHour:
				t, err := time.ParseInLocation(model.HourFormat, child.Name, time.Local)
				if err != nil {
					continue
				}
				tableTime = t
			case model.RotatorPartDay:
				t, err := time.ParseInLocation(model.DayFormat, child.Name, time.Local)
				if err != nil {
					continue
				}
				tableTime = t
			case model.RotatorPartMonth:
				t, err := time.ParseInLocation(model.MonthFormat, child.Name, time.Local)
				if err != nil {
					continue
				}
				tableTime = t
			default:
				continue
			}
			if tableTime.Before(baseTime) {
				s.logger.Infof("Old table need to be deleted %v, table time: %v, base time: %v", child.Name, tableTime, baseTime)
				if err := s.db.Table().Do(context.Background(), func(ctx context.Context, session ydb_table.Session) error {
					dropTable := DropTableTemplate{s.getFullPath(currPath.MakeChildPath(child.Name))}

					var query strings.Builder
					if err := dropTableQueryTemplate.Execute(&query, dropTable); err != nil {
						return xerrors.Errorf("unable to execute drop table template:\n %w", err)
					}

					ctx, cancel := context.WithTimeout(ctx, time.Minute)
					defer cancel()

					return session.ExecuteSchemeQuery(ctx, query.String())
				}); err != nil {
					s.logger.Warn("Unable to delete table", log.Error(err))
					continue
				}
			} else {
				childPath := s.getFullPath(currPath.MakeChildPath(child.Name))
				nextTablePath := ydbPath(s.config.Rotation.Next(string(currPath)))
				if err := s.db.Table().Do(context.TODO(), func(ctx context.Context, session ydb_table.Session) error {
					desc, err := session.DescribeTable(ctx, childPath)
					if err != nil {
						return xerrors.Errorf("Cannot describe table %s: %w", childPath, err)
					}
					if err := s.checkTable(nextTablePath, abstract.NewTableSchema(FromYdbSchema(desc.Columns, desc.PrimaryKey))); err != nil {
						s.logger.Warn("Unable to init clone", log.Error(err))
					}

					return nil
				}); err != nil {
					s.logger.Warnf("Unable to init next table %s: %v", nextTablePath, err)
					continue
				}
			}
		}
	}
}

func (s *sinker) runRotator() {
	defer s.Close()
	for {
		if s.isClosed() {
			return
		}

		if err := s.rotateTable(); err != nil {
			s.logger.Warn("runRotator err", log.Error(err))
		}
		time.Sleep(5 * time.Minute)
	}
}

func (s *sinker) getTablePathForChangeItem(item abstract.ChangeItem) ydbPath {
	tableName := Fqtn(item.TableID())

	if altName, ok := s.config.AltNames[item.Fqtn()]; ok {
		tableName = altName
	} else if altName, ok = s.config.AltNames[tableName]; ok {
		// for backward compatibility need to check both name and old Fqtn
		tableName = altName
	}
	tablePath := ydbPath(tableName)
	if item.Kind == abstract.InsertKind || item.Kind == abstract.UpdateKind || item.Kind == abstract.DeleteKind {
		tablePath = ydbPath(s.config.Rotation.AnnotateWithTimeFromColumn(tableName, item))
	}
	if s.config.Path != "" {
		tablePath = ydbPath(path.Join(s.config.Path, string(tablePath)))
	}
	return tablePath
}

func (s *sinker) Push(input []abstract.ChangeItem) error {
	batches := make(map[ydbPath][]abstract.ChangeItem)
	for _, item := range input {
		switch item.Kind {
		// Truncate - implemented as drop
		case abstract.DropTableKind, abstract.TruncateTableKind:
			tablePath := s.getTablePathForChangeItem(item)
			if s.config.Cleanup == model.DisabledCleanup {
				s.logger.Infof("Skipped dropping/truncating table '%v' due cleanup policy", s.getFullPath(tablePath))
				continue
			}
			exists, err := sugar.IsEntryExists(context.Background(), s.db.Scheme(), s.getFullPath(tablePath), ydb_scheme.EntryTable, ydb_scheme.EntryColumnTable)
			if err != nil {
				return xerrors.Errorf("unable to check table existence %s: %w", s.getFullPath(tablePath), err)
			}

			if !exists {
				return nil
			}

			s.logger.Infof("try to drop table: %v", s.getFullPath(tablePath))
			if err := s.db.Table().Do(context.Background(), func(ctx context.Context, session ydb_table.Session) error {
				dropTable := DropTableTemplate{s.getFullPath(tablePath)}

				var query strings.Builder
				if err := dropTableQueryTemplate.Execute(&query, dropTable); err != nil {
					return xerrors.Errorf("unable to execute drop table template:\n %w", err)
				}

				ctx, cancel := context.WithTimeout(ctx, time.Minute)
				defer cancel()

				return session.ExecuteSchemeQuery(ctx, query.String())
			}); err != nil {
				s.logger.Warn("Unable to delete table", log.Error(err))

				return xerrors.Errorf("unable to drop table %s: %w", s.getFullPath(tablePath), err)
			}
		case abstract.InsertKind, abstract.UpdateKind, abstract.DeleteKind:
			tablePath := s.getTablePathForChangeItem(item)
			batches[tablePath] = append(batches[tablePath], item)
		case abstract.SynchronizeKind:
			// do nothing
		default:
			s.logger.Infof("kind: %v not supported", item.Kind)
		}
	}
	wg := sync.WaitGroup{}
	var batchErrs []error
	for tablePath, batch := range batches {
		if err := s.checkTable(tablePath, batch[0].TableSchema); err != nil {
			if err == SchemaMismatchErr {
				time.Sleep(time.Second)
				if err := s.checkTable(tablePath, batch[0].TableSchema); err != nil {
					s.logger.Error("Check table error", log.Error(err))
					batchErrs = append(batchErrs, xerrors.Errorf("unable to check table %s: %w", tablePath, err))
				}
			} else {
				s.logger.Error("Check table error", log.Error(err))
				batchErrs = append(batchErrs, err)
			}
		}
		// The most fragile part of Collape is processing PK changing events.
		// Here we transform these changes into Delete + Insert pair and only then send batch to Collapse
		// As a result potentially dangerous part of Collapse is avoided + PK updates are processed correctly (it is imposible to update pk in YDB explicitly)
		// Ticket about rewriting Collapse https://st.yandex-team.ru/TM-8239
		chunks := splitToChunks(abstract.Collapse(s.processPKUpdate(batch)))
		for _, chunk := range chunks {
			wg.Add(1)
			go func(tablePath ydbPath, chunk []abstract.ChangeItem) {
				defer wg.Done()
				if err := s.pushBatch(tablePath, chunk); err != nil {
					msg := fmt.Sprintf("Unable to push %d items into table %s", len(chunk), tablePath)
					batchErrs = append(batchErrs, xerrors.Errorf("%s: %w", msg, err))
					logger.Log.Error(msg, log.Error(err))
				}
			}(tablePath, chunk)
		}
	}
	wg.Wait()
	if err := errors.Join(batchErrs...); err != nil {
		return xerrors.Errorf("unable to proceed input batch: %w", err)
	}

	return nil
}

func splitToChunks(items []abstract.ChangeItem) [][]abstract.ChangeItem {
	var res [][]abstract.ChangeItem
	batchSize := uint64(0)
	left := 0
	for right := range len(items) {
		batchSize += items[right].Size.Read
		if batchSize >= writeBatchMaxSize || right-left >= writeBatchMaxLen {
			res = append(res, items[left:right+1])
			batchSize = 0
			left = right + 1
		}
	}
	if left < len(items) {
		res = append(res, items[left:])
	}
	return res
}

func (s *sinker) processPKUpdate(batch []abstract.ChangeItem) []abstract.ChangeItem {
	parts := abstract.SplitUpdatedPKeys(batch)
	result := make([]abstract.ChangeItem, 0)
	for _, part := range parts {
		result = append(result, part...)
	}
	return result
}

func (s *sinker) pushBatch(tablePath ydbPath, batch []abstract.ChangeItem) error {
	retries := uint64(5)
	regular := make([]abstract.ChangeItem, 0)
	for _, ci := range batch {
		if ci.Kind == abstract.DeleteKind {
			if err := backoff.Retry(func() error {
				err := s.delete(tablePath, ci)
				if err != nil {
					s.logger.Error("Delete error", log.Error(err))
					return xerrors.Errorf("unable to delete: %w", err)
				}
				return nil
			}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), retries)); err != nil {
				s.metrics.Table(string(tablePath), "error", 1)
				return xerrors.Errorf("unable to delete %s (tried %d times): %w", string(tablePath), retries, err)
			}
			continue
		}
		if len(ci.ColumnNames) == len(ci.TableSchema.Columns()) {
			regular = append(regular, ci)
		} else {
			if err := backoff.Retry(func() error {
				err := s.insert(tablePath, []abstract.ChangeItem{ci})
				if err != nil {
					return xerrors.Errorf("unable to upsert toasted row: %w", err)
				}
				return nil
			}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), retries)); err != nil {
				s.metrics.Table(string(tablePath), "error", 1)
				return xerrors.Errorf("unable to upsert toasted, %v retries exceeded: %w", retries, err)
			}
		}
	}
	if err := backoff.Retry(func() error {
		err := s.insert(tablePath, regular)
		if err != nil {
			if s.isClosed() {
				return backoff.Permanent(err)
			}
			return xerrors.Errorf("unable to upsert toasted row: %w", err)
		}
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), retries)); err != nil {
		s.metrics.Table(string(tablePath), "error", 1)

		return xerrors.Errorf("unable to insert %v rows, %v retries exceeded: %w", len(regular), retries, err)
	}
	s.metrics.Table(string(tablePath), "rows", len(batch))
	return nil
}

func (s *sinker) deleteQuery(tablePath ydbPath, keySchemas []abstract.ColSchema) string {
	cols := make([]TemplateCol, len(keySchemas))
	for i, c := range keySchemas {
		cols[i].Name = c.ColumnName
		cols[i].Typ = s.ydbType(c.DataType, c.OriginalType).Yql()
		if i != len(keySchemas)-1 {
			cols[i].Comma = ","
		}
		if c.Required {
			cols[i].Optional = ""
		} else {
			cols[i].Optional = "?"
		}
	}
	if s.config.ShardCount > 0 {
		cols[len(cols)-1].Comma = ","
		cols = append(cols, TemplateCol{
			Name:     "_shard_key",
			Typ:      "Uint64",
			Optional: "?",
			Comma:    "",
		})
	}
	buf := new(bytes.Buffer)
	_ = deleteTemplate.Execute(buf, &TemplateModel{Cols: cols, Path: string(tablePath)})
	return buf.String()
}

func (s *sinker) insertQuery(tablePath ydbPath, colSchemas []abstract.ColSchema) string {
	cols := make([]TemplateCol, len(colSchemas))
	for i, c := range colSchemas {
		cols[i].Name = c.ColumnName
		cols[i].Typ = s.adjustTypName(c.DataType)
		if i != len(colSchemas)-1 {
			cols[i].Comma = ","
		}
		if c.Required {
			cols[i].Optional = ""
		} else {
			cols[i].Optional = "?"
		}
	}
	if s.config.ShardCount > 0 {
		cols[len(cols)-1].Comma = ","
		cols = append(cols, TemplateCol{
			Name:     "_shard_key",
			Typ:      "Uint64",
			Optional: "?",
			Comma:    "",
		})
	}
	buf := new(bytes.Buffer)
	_ = insertTemplate.Execute(buf, &TemplateModel{Cols: cols, Path: string(tablePath)})
	return buf.String()
}

func (s *sinker) insert(tablePath ydbPath, batch []abstract.ChangeItem) error {
	if len(batch) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	colSchemas := batch[0].TableSchema.Columns()
	rev := make(map[string]int)
	for i, v := range colSchemas {
		rev[v.ColumnName] = i
	}
	rows := make([]ydb_table_types.Value, len(batch))
	var finalSchema []abstract.ColSchema
	for _, c := range batch[0].ColumnNames {
		finalSchema = append(finalSchema, colSchemas[rev[c]])
	}
	for i, r := range batch {
		fields := make([]ydb_table_types.StructValueOption, 0)
		for j, c := range r.ColumnNames {
			val, opt, err := s.ydbVal(colSchemas[rev[c]].DataType, colSchemas[rev[c]].OriginalType, r.ColumnValues[j])
			if err != nil {
				return xerrors.Errorf("%s: unable to build val: %w", c, err)
			}
			if !colSchemas[rev[c]].Required && !opt {
				val = ydb_table_types.OptionalValue(val)
			}
			fields = append(fields, ydb_table_types.StructFieldValue(c, val))
		}
		if s.config.ShardCount > 0 {
			var cs uint64
			switch v := r.ColumnValues[0].(type) {
			case string:
				cs = crc64.Checksum([]byte(v))
			default:
				cs = crc64.Checksum([]byte(fmt.Sprintf("%v", v)))
			}
			fields = append(fields, ydb_table_types.StructFieldValue("_shard_key", ydb_table_types.OptionalValue(ydb_table_types.Uint64Value(cs))))
		}
		rows[i] = ydb_table_types.StructValue(fields...)
	}

	batchList := ydb_table_types.ListValue(rows...)
	if s.config.LegacyWriter {
		writeTx := ydb_table.TxControl(
			ydb_table.BeginTx(
				ydb_table.WithSerializableReadWrite(),
			),
			ydb_table.CommitTx(),
		)
		err := s.db.Table().Do(ctx, func(ctx context.Context, session ydb_table.Session) (err error) {
			q := s.insertQuery(tablePath, finalSchema)
			s.logger.Debug(q)
			stmt, err := session.Prepare(ctx, q)
			if err != nil {
				s.logger.Warn(fmt.Sprintf("Unable to prepare insert query:\n%v", q))
				return xerrors.Errorf("unable to prepare insert query: %w", err)
			}
			_, _, err = stmt.Execute(ctx, writeTx, ydb_table.NewQueryParameters(
				ydb_table.ValueParam("$batch", batchList),
			))
			if err != nil {
				s.logger.Warn(fmt.Sprintf("unable to execute:\n%v", q), log.Error(err))
				return xerrors.Errorf("unable to execute: %w", err)
			}
			return nil
		})
		if err != nil {
			return xerrors.Errorf("unable to insert with legacy writer:\n %w", err)
		}

		return nil
	}

	tableFullPath := s.getFullPath(tablePath)
	bulkUpsertBatch := ydb_table.BulkUpsertDataRows(batchList)
	if err := s.db.Table().BulkUpsert(ctx, tableFullPath, bulkUpsertBatch); err != nil {
		s.logger.Warn("unable to upload rows", log.Error(err), log.String("table", tableFullPath))
		if s.config.IgnoreRowTooLargeErrors && rowTooLargeRegexp.MatchString(err.Error()) {
			s.logger.Warn("ignoring row too large error as per IgnoreRowTooLargeErrors option")
			return nil
		}
		return xerrors.Errorf("unable to bulk upsert table %v: %w", tableFullPath, err)
	}

	return nil
}

func (s *sinker) fitTime(t time.Time) (time.Time, error) {
	if t.Sub(time.Unix(0, 0)) < 0 {
		if s.config.FitDatetime {
			// we looze some data here
			return time.Unix(0, 0), nil
		}
		return time.Time{}, xerrors.Errorf("time value is %v, minimum: %v", t, time.Unix(0, 0))
	}
	return t, nil
}

func (s *sinker) extractTimeValue(val *time.Time, dataType, originalType string) (ydb_table_types.Value, error) {
	if val == nil {
		return ydb_table_types.NullValue(s.ydbType(dataType, originalType)), nil
	}

	fitTime, err := s.fitTime(*val)
	if err != nil {
		return nil, xerrors.Errorf("Time not fit YDB restriction: %w", err)
	}

	switch ytschema.Type(dataType) {
	case ytschema.TypeDate:
		return ydb_table_types.DateValueFromTime(fitTime), nil
	case ytschema.TypeDatetime:
		return ydb_table_types.DatetimeValueFromTime(fitTime), nil
	case ytschema.TypeTimestamp:
		return ydb_table_types.TimestampValueFromTime(fitTime), nil
	}
	return nil, xerrors.Errorf("unable to marshal %s value (%v) as a time type", dataType, val)
}

func (s *sinker) ydbVal(dataType, originalType string, val interface{}) (ydb_table_types.Value, bool, error) {
	if val == nil {
		return ydb_table_types.NullValue(s.ydbType(dataType, originalType)), true, nil
	}

	switch originalType {
	case "ydb:DyNumber":
		switch v := val.(type) {
		case string:
			if s.config.IsTableColumnOriented {
				return ydb_table_types.StringValueFromString(v), false, nil
			}
			return ydb_table_types.DyNumberValue(v), false, nil
		case json.Number:
			if s.config.IsTableColumnOriented {
				return ydb_table_types.StringValueFromString(v.String()), false, nil
			}
			return ydb_table_types.DyNumberValue(v.String()), false, nil
		}
	case "ydb:Decimal":
		valStr := val.(string)
		if s.config.IsTableColumnOriented {
			return ydb_table_types.StringValueFromString(valStr), false, nil
		}
		v, err := ydb_decimal.Parse(valStr, 22, 9)
		if err != nil {
			return nil, true, xerrors.Errorf("unable to parse decimal number: %s", valStr)
		}
		return ydb_table_types.DecimalValueFromBigInt(v, 22, 9), false, nil
	case "ydb:Interval":
		var duration time.Duration
		switch v := val.(type) {
		case time.Duration:
			duration = val.(time.Duration)
		case int64:
			duration = time.Duration(v)
		case json.Number:
			result, err := v.Int64()
			if err != nil {
				return nil, true, xerrors.Errorf("unable to extract int64 from json.Number: %s", v.String())
			}
			duration = time.Duration(result)
		default:
			return nil, true, xerrors.Errorf("unknown ydb:Interval type: %T", val)
		}
		if s.config.IsTableColumnOriented {
			return ydb_table_types.Int64Value(duration.Nanoseconds()), false, nil
		}
		return ydb_table_types.IntervalValueFromDuration(duration), false, nil
	case "ydb:Datetime":
		switch vv := val.(type) {
		case time.Time:
			return ydb_table_types.DatetimeValueFromTime(vv), false, nil
		case *time.Time:
			if vv != nil {
				return ydb_table_types.DatetimeValueFromTime(*vv), false, nil
			}
			return ydb_table_types.NullValue(s.ydbType(dataType, originalType)), true, nil
		default:
			return nil, true, xerrors.Errorf("Unable to marshal timestamp value: %v with type: %T", vv, vv)
		}
	case "ydb:Date":
		switch vv := val.(type) {
		case time.Time:
			return ydb_table_types.DateValueFromTime(vv), false, nil
		case *time.Time:
			if vv != nil {
				return ydb_table_types.DateValueFromTime(*vv), false, nil
			}
			return ydb_table_types.NullValue(s.ydbType(dataType, originalType)), true, nil
		default:
			return nil, true, xerrors.Errorf("Unable to marshal timestamp value: %v with type: %T", vv, vv)
		}
	case "ydb:Uuid":
		switch vv := val.(type) {
		case string:
			if s.config.IsTableColumnOriented {
				return ydb_table_types.UTF8Value(vv), false, nil
			}
			uuidVal, err := uuid.Parse(vv)
			if err != nil {
				return nil, true, xerrors.Errorf("Unable to parse UUID value: %w", err)
			}
			return ydb_table_types.UuidValue(uuidVal), false, nil
		default:
			return nil, true, xerrors.Errorf("unknown ydb:Uuid type: %T, val=%s", val, val)
		}
	}
	if !s.config.IsTableColumnOriented {
		switch originalType {
		case "ydb:Int8":
			switch vv := val.(type) {
			case int8:
				return ydb_table_types.Int8Value(int8(vv)), false, nil
			default:
				return nil, true, xerrors.Errorf("Unable to convert %s value: %v with type: %T", originalType, vv, vv)
			}
		case "ydb:Int16":
			switch vv := val.(type) {
			case int16:
				return ydb_table_types.Int16Value(int16(vv)), false, nil
			default:
				return nil, true, xerrors.Errorf("Unable to convert %s value: %v with type: %T", originalType, vv, vv)
			}
		case "ydb:Uint16":
			switch vv := val.(type) {
			case uint16:
				return ydb_table_types.Uint16Value(uint16(vv)), false, nil
			default:
				return nil, true, xerrors.Errorf("Unable to convert %s value: %v with type: %T", originalType, vv, vv)
			}
		}
	}

	switch dataType {
	case "DateTime":
		return ydb_table_types.DatetimeValueFromTime(val.(time.Time)), false, nil
	default:
		switch ytschema.Type(dataType) {
		case ytschema.TypeDate, ytschema.TypeDatetime, ytschema.TypeTimestamp:
			switch vv := val.(type) {
			case time.Time:
				value, err := s.extractTimeValue(&vv, dataType, originalType)
				if err != nil {
					return nil, false, xerrors.Errorf("unable to extract %s value: %w ", dataType, err)
				}
				return value, false, nil
			case *time.Time:
				value, err := s.extractTimeValue(vv, dataType, originalType)
				if err != nil {
					return nil, false, xerrors.Errorf("unable to extract %s value: %w ", dataType, err)
				}
				return value, true, nil
			default:
				return nil, false, xerrors.Errorf("unable to marshal %s value: %v with type: %T",
					ytschema.Type(dataType), vv, vv)
			}
		case ytschema.TypeAny:
			var data []byte
			var err error
			if originalType == "ydb:Yson" {
				data, err = yson.Marshal(val)
				if err != nil {
					return nil, false, xerrors.Errorf("unable to yson marshal: %w", err)
				}
			} else {
				data, err = json.Marshal(val)
				if err != nil {
					return nil, false, xerrors.Errorf("unable to json marshal: %w", err)
				}
			}
			switch originalType {
			case "ydb:Yson":
				return ydb_table_types.YSONValueFromBytes(data), false, nil
			case "ydb:Json":
				return ydb_table_types.JSONValueFromBytes(data), false, nil
			case "ydb:JsonDocument":
				return ydb_table_types.JSONDocumentValueFromBytes(data), false, nil
			default:
				return ydb_table_types.JSONValueFromBytes(data), false, nil
			}
		case ytschema.TypeBytes:
			switch v := val.(type) {
			case string:
				return ydb_table_types.BytesValue([]byte(v)), false, nil
			case []uint8:
				return ydb_table_types.BytesValue(v), false, nil
			default:
				r, err := json.Marshal(val)
				if err != nil {
					return nil, false, xerrors.Errorf("unable to json marshal: %w", err)
				}
				return ydb_table_types.BytesValue(r), false, nil
			}
		case ytschema.TypeString:
			switch v := val.(type) {
			case string:
				return ydb_table_types.UTF8Value(v), false, nil
			case time.Time:
				return ydb_table_types.UTF8Value(v.String()), false, nil
			case uuid.UUID:
				return ydb_table_types.UTF8Value(v.String()), false, nil
			default:
				r, err := json.Marshal(val)
				if err != nil {
					return nil, false, xerrors.Errorf("unable to json marshal: %w", err)
				}
				return ydb_table_types.UTF8Value(string(r)), false, nil
			}
		case ytschema.TypeFloat32:
			switch t := val.(type) {
			case float64:
				return ydb_table_types.FloatValue(float32(t)), false, nil
			case float32:
				return ydb_table_types.FloatValue(t), false, nil
			case json.Number:
				valDouble, err := t.Float64()
				if err != nil {
					return nil, true, xerrors.Errorf("unable to convert json.Number to double: %s", t.String())
				}
				return ydb_table_types.FloatValue(float32(valDouble)), false, nil
			default:
				return nil, true, xerrors.Errorf("unexpected data type: %T for: %s", val, dataType)
			}
		case ytschema.TypeFloat64:
			switch t := val.(type) {
			case float64:
				return ydb_table_types.DoubleValue(t), false, nil
			case float32:
				return ydb_table_types.DoubleValue(float64(t)), false, nil
			case *json.Number:
				valDouble, err := t.Float64()
				if err != nil {
					return nil, true, xerrors.Errorf("unable to convert *json.Number to double: %s", t.String())
				}
				return ydb_table_types.DoubleValue(valDouble), false, nil
			case json.Number:
				valDouble, err := t.Float64()
				if err != nil {
					return nil, true, xerrors.Errorf("unable to convert json.Number to double: %s", t.String())
				}
				return ydb_table_types.DoubleValue(valDouble), false, nil
			default:
				return nil, true, xerrors.Errorf("unexpected data type: %T for: %s", val, dataType)
			}
		case ytschema.TypeBoolean:
			asBool := val.(bool)
			if s.config.IsTableColumnOriented {
				asUint := uint8(0)
				if asBool {
					asUint = uint8(1)
				}
				return ydb_table_types.Uint8Value(asUint), false, nil
			}
			return ydb_table_types.BoolValue(asBool), false, nil
		case ytschema.TypeInt32, ytschema.TypeInt16, ytschema.TypeInt8:
			switch t := val.(type) {
			case int:
				return ydb_table_types.Int32Value(int32(t)), false, nil
			case int8:
				return ydb_table_types.Int32Value(int32(t)), false, nil
			case int16:
				return ydb_table_types.Int32Value(int32(t)), false, nil
			case int32:
				return ydb_table_types.Int32Value(t), false, nil
			case int64:
				return ydb_table_types.Int32Value(int32(t)), false, nil
			default:
				return nil, true, xerrors.Errorf("unexpected data type: %T for: %s", val, dataType)
			}
		case ytschema.TypeInt64:
			switch t := val.(type) {
			case int:
				return ydb_table_types.Int64Value(int64(t)), false, nil
			case int64:
				return ydb_table_types.Int64Value(t), false, nil
			default:
				return nil, true, xerrors.Errorf("unexpected data type: %T for: %s", val, dataType)
			}
		case ytschema.TypeUint8:
			switch t := val.(type) {
			case int:
				return ydb_table_types.Uint8Value(uint8(t)), false, nil
			case uint8:
				return ydb_table_types.Uint8Value(t), false, nil
			default:
				return nil, true, xerrors.Errorf("unexpected data type: %T for: %s", val, dataType)
			}
		case ytschema.TypeUint32, ytschema.TypeUint16:
			switch t := val.(type) {
			case int:
				return ydb_table_types.Uint32Value(uint32(t)), false, nil
			case uint16:
				return ydb_table_types.Uint32Value(uint32(t)), false, nil
			case uint32:
				return ydb_table_types.Uint32Value(t), false, nil
			default:
				return nil, true, xerrors.Errorf("unexpected data type: %T for: %s", val, dataType)
			}
		case ytschema.TypeUint64:
			switch t := val.(type) {
			case int:
				return ydb_table_types.Uint64Value(uint64(t)), false, nil
			case uint64:
				return ydb_table_types.Uint64Value(t), false, nil
			default:
				return nil, true, xerrors.Errorf("unexpected data type: %T for: %s", val, dataType)
			}
		case ytschema.TypeInterval:
			switch t := val.(type) {
			case time.Duration:
				if s.config.IsTableColumnOriented {
					return ydb_table_types.Int64Value(t.Nanoseconds()), false, nil
				}
				// what the point in losing accuracy?
				return ydb_table_types.IntervalValueFromMicroseconds(t.Microseconds()), false, nil
			default:
				return nil, true, xerrors.Errorf("unexpected data type: %T for: %s", val, dataType)
			}
		default:
			return nil, true, xerrors.Errorf("unexpected data type: %T for: %s", val, dataType)
		}
	}
}

func (s *sinker) ydbType(dataType, originalType string) ydb_table_types.Type {
	if s.config.IsTableColumnOriented && strings.HasPrefix(originalType, "ydb:Tz") {
		// btw looks like Tz* and uuid params are not supported due to lack of conversion in ydbVal func
		// tests are passing due to those types being commented
		return ydb_table_types.TypeUnknown
	}
	if strings.HasPrefix(originalType, "ydb:") {
		originalTypeStr := strings.TrimPrefix(originalType, "ydb:")
		switch originalTypeStr {
		case "Bool":
			if s.config.IsTableColumnOriented {
				return ydb_table_types.TypeUint8
			}
			return ydb_table_types.TypeBool
		case "Int8":
			if s.config.IsTableColumnOriented {
				return ydb_table_types.TypeInt32
			}
			return ydb_table_types.TypeInt8
		case "Uint8":
			return ydb_table_types.TypeUint8
		case "Int16":
			if s.config.IsTableColumnOriented {
				return ydb_table_types.TypeInt32
			}
			return ydb_table_types.TypeInt16
		case "Uint16":
			if s.config.IsTableColumnOriented {
				return ydb_table_types.TypeUint32
			}
			return ydb_table_types.TypeUint16
		case "Int32":
			return ydb_table_types.TypeInt32
		case "Uint32":
			return ydb_table_types.TypeUint32
		case "Int64":
			return ydb_table_types.TypeInt64
		case "Uint64":
			return ydb_table_types.TypeUint64
		case "Float":
			return ydb_table_types.TypeFloat
		case "Double":
			return ydb_table_types.TypeDouble
		case "Decimal":
			if s.config.IsTableColumnOriented {
				return ydb_table_types.TypeString
			}
			return TypeYdbDecimal
		case "Date":
			return ydb_table_types.TypeDate
		case "Datetime":
			return ydb_table_types.TypeDatetime
		case "Timestamp":
			return ydb_table_types.TypeTimestamp
		case "Interval":
			if s.config.IsTableColumnOriented {
				return ydb_table_types.TypeInt64
			}
			return ydb_table_types.TypeInterval
		case "TzDate":
			return ydb_table_types.TypeTzDate
		case "TzDatetime":
			return ydb_table_types.TypeTzDatetime
		case "TzTimestamp":
			return ydb_table_types.TypeTzTimestamp
		case "String":
			return ydb_table_types.TypeString
		case "Utf8":
			return ydb_table_types.TypeUTF8
		case "Yson":
			return ydb_table_types.TypeYSON
		case "Json":
			return ydb_table_types.TypeJSON
		case "Uuid":
			if s.config.IsTableColumnOriented {
				return ydb_table_types.TypeUTF8
			}
			return ydb_table_types.TypeUUID
		case "JsonDocument":
			return ydb_table_types.TypeJSONDocument
		case "DyNumber":
			if s.config.IsTableColumnOriented {
				return ydb_table_types.TypeString
			}
			return ydb_table_types.TypeDyNumber
		default:
			return ydb_table_types.TypeUnknown
		}
	}

	switch dataType {
	case "DateTime":
		return ydb_table_types.TypeDatetime
	default:
		switch ytschema.Type(dataType) {
		case ytschema.TypeInterval:
			if s.config.IsTableColumnOriented {
				return ydb_table_types.TypeInt64
			}
			return ydb_table_types.TypeInterval
		case ytschema.TypeDate:
			return ydb_table_types.TypeDate
		case ytschema.TypeDatetime:
			return ydb_table_types.TypeDatetime
		case ytschema.TypeTimestamp:
			return ydb_table_types.TypeTimestamp
		case ytschema.TypeAny:
			return ydb_table_types.TypeJSON
		case ytschema.TypeString:
			return ydb_table_types.TypeUTF8
		case ytschema.TypeBytes:
			return ydb_table_types.TypeString
		case ytschema.TypeFloat32:
			return ydb_table_types.TypeFloat
		case ytschema.TypeFloat64:
			return ydb_table_types.TypeDouble
		case ytschema.TypeBoolean:
			if s.config.IsTableColumnOriented {
				return ydb_table_types.TypeUint8
			}
			return ydb_table_types.TypeBool
		case ytschema.TypeInt32, ytschema.TypeInt16, ytschema.TypeInt8:
			return ydb_table_types.TypeInt32
		case ytschema.TypeInt64:
			return ydb_table_types.TypeInt64
		case ytschema.TypeUint8:
			return ydb_table_types.TypeUint8
		case ytschema.TypeUint32, ytschema.TypeUint16:
			return ydb_table_types.TypeUint32
		case ytschema.TypeUint64:
			return ydb_table_types.TypeUint64
		default:
			return ydb_table_types.TypeUnknown
		}
	}
}

func (s *sinker) isPrimaryKey(ydbType ydb_table_types.Type, column abstract.ColSchema) (bool, error) {
	if !column.PrimaryKey {
		return false, nil
	}
	allowedIn, ok := primaryIsAllowedFor[ydbType]
	var res bool
	if !ok {
		res = false
	} else if s.config.IsTableColumnOriented {
		res = allowedIn != OLTP
	} else {
		res = allowedIn != OLAP
	}
	if res {
		return true, nil
	} else {
		// we should drop transfer activation if we can't create primary key with column that supposed to be in pk
		// due to possibility to lose data if table has complex pk, consisting of several columns
		ydbTypesURL := "https://ydb.tech/en/docs/yql/reference/types/primitive"
		if s.config.IsTableColumnOriented {
			ydbTypesURL = "https://ydb.tech/en/docs/concepts/column-table#olap-data-types"
		}
		return false, xerrors.Errorf(
			"Column %s is in a primary key in source db, but can't be a pk in ydb due to its type being %v. Check documentation about supported types for pk here %s",
			column.TableName,
			ydbType,
			ydbTypesURL,
		)
	}
}

func (s *sinker) adjustTypName(typ string) string {
	switch typ {
	case "DateTime":
		return "Datetime"
	default:
		switch ytschema.Type(typ) {
		case ytschema.TypeInterval:
			if s.config.IsTableColumnOriented {
				return "Int64"
			}
			return "Interval"
		case ytschema.TypeDate:
			return "Date"
		case ytschema.TypeDatetime:
			return "Datetime"
		case ytschema.TypeTimestamp:
			return "Timestamp"
		case ytschema.TypeAny:
			return "Json"
		case ytschema.TypeString:
			return "Utf8"
		case ytschema.TypeBytes:
			return "String"
		case ytschema.TypeFloat64:
			// TODO What to do with real float?
			return "Double"
		case ytschema.TypeBoolean:
			if s.config.IsTableColumnOriented {
				return "Uint8"
			}
			return "Bool"
		case ytschema.TypeInt32, ytschema.TypeInt16, ytschema.TypeInt8:
			return "Int32"
		case ytschema.TypeInt64:
			return "Int64"
		case ytschema.TypeUint8:
			return "Uint8"
		case ytschema.TypeUint32, ytschema.TypeUint16:
			return "Uint32"
		case ytschema.TypeUint64:
			return "Uint64"
		default:
			return "Unknown"
		}
	}
}

func (s *sinker) delete(tablePath ydbPath, item abstract.ChangeItem) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	colSchemas := item.TableSchema.Columns()
	rev := make(map[string]int)
	for i, v := range colSchemas {
		rev[v.ColumnName] = i
	}
	var finalSchema []abstract.ColSchema
	for _, c := range item.OldKeys.KeyNames {
		finalSchema = append(finalSchema, colSchemas[rev[c]])
	}
	fields := make([]ydb_table_types.StructValueOption, 0)
	for i, c := range item.OldKeys.KeyNames {
		val, opt, err := s.ydbVal(colSchemas[rev[c]].DataType, colSchemas[rev[c]].OriginalType, item.OldKeys.KeyValues[i])
		if err != nil {
			return xerrors.Errorf("unable to build ydb val: %w", err)
		}
		if !colSchemas[rev[c]].Required && !opt {
			val = ydb_table_types.OptionalValue(val)
		}
		fields = append(fields, ydb_table_types.StructFieldValue(c, val))
	}
	if s.config.ShardCount > 0 {
		var cs uint64
		switch v := item.ColumnValues[0].(type) {
		case string:
			cs = crc64.Checksum([]byte(v))
		default:
			cs = crc64.Checksum([]byte(fmt.Sprintf("%v", v)))
		}
		fields = append(fields, ydb_table_types.StructFieldValue("_shard_key", ydb_table_types.OptionalValue(ydb_table_types.Uint64Value(cs))))
	}
	batch := ydb_table_types.StructValue(fields...)
	writeTx := ydb_table.TxControl(
		ydb_table.BeginTx(
			ydb_table.WithSerializableReadWrite(),
		),
		ydb_table.CommitTx(),
	)

	return s.db.Table().Do(ctx, func(ctx context.Context, session ydb_table.Session) (err error) {
		q := s.deleteQuery(tablePath, finalSchema)
		s.logger.Debug(q)
		stmt, err := session.Prepare(ctx, q)
		if err != nil {
			s.logger.Warn(fmt.Sprintf("Unable to prepare delete query:\n%v", q))
			return xerrors.Errorf("unable to prepare delete query: %w", err)
		}
		_, _, err = stmt.Execute(ctx, writeTx, ydb_table.NewQueryParameters(
			ydb_table.ValueParam("$batch", batch),
		))
		if err != nil {
			s.logger.Warn(fmt.Sprintf("unable to execute:\n%v", q), log.Error(err))
			return xerrors.Errorf("unable to execute delete: %w", err)
		}
		return nil
	})
}

func NewSinker(lgr log.Logger, cfg *YdbDestination, mtrcs core_metrics.Registry) (abstract.Sinker, error) {
	var err error
	var tlsConfig *tls.Config
	if cfg.TLSEnabled {
		tlsConfig, err = xtls.FromPath(cfg.RootCAFiles)
		if err != nil {
			return nil, xerrors.Errorf("could not create TLS config: %w", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var creds ydb_credentials.Credentials
	creds, err = ResolveCredentials(
		cfg.UserdataAuth,
		string(cfg.Token),
		JWTAuthParams{
			KeyContent:      cfg.SAKeyContent,
			TokenServiceURL: cfg.TokenServiceURL,
		},
		cfg.ServiceAccountID,
		cfg.OAuth2Config,
		logger.Log,
	)
	if err != nil {
		return nil, xerrors.Errorf("Cannot create YDB credentials: %w", err)
	}

	ydbDriver, err := newYDBDriver(ctx, cfg.Database, cfg.Instance, creds, tlsConfig)
	if err != nil {
		return nil, xerrors.Errorf("unable to init ydb driver: %w", err)
	}

	s := &sinker{
		db:      ydbDriver,
		config:  cfg,
		logger:  lgr,
		metrics: stats.NewSinkerStats(mtrcs),
		locks:   sync.Mutex{},
		lock:    sync.Mutex{},
		cache:   make(map[ydbPath]*abstract.TableSchema),
		once:    sync.Once{},
		closeCh: make(chan struct{}),
	}
	if s.config.Rotation != nil && s.config.Primary {
		go s.runRotator()
	}
	return s, nil
}
