package ydb

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/jsonx"
	"github.com/transferia/transferia/pkg/xtls"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yson"
)

type Storage struct {
	config  *YdbStorageParams
	db      *ydb.Driver
	metrics *stats.SourceStats
}

func NewStorage(cfg *YdbStorageParams, mtrcs metrics.Registry) (*Storage, error) {
	var err error
	var tlsConfig *tls.Config
	if cfg.TLSEnabled {
		tlsConfig, err = xtls.FromPath(cfg.RootCAFiles)
		if err != nil {
			return nil, xerrors.Errorf("Cannot create TLS config: %w", err)
		}
	}
	clientCtx, cancel := context.WithTimeout(context.Background(), time.Minute*3)
	defer cancel()

	var ydbCreds credentials.Credentials
	ydbCreds, err = ResolveCredentials(
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

	ydbDriver, err := newYDBDriver(clientCtx, cfg.Database, cfg.Instance, ydbCreds, tlsConfig, false)
	if err != nil {
		return nil, xerrors.Errorf("Cannot create YDB driver: %w", err)
	}

	return &Storage{
		config:  cfg,
		db:      ydbDriver,
		metrics: stats.NewSourceStats(mtrcs),
	}, nil
}

func (s *Storage) Close() {
}

func (s *Storage) Ping() error {
	return nil
}

func (s *Storage) traverse(directoryPath string) ([]string, error) {
	parent, err := s.db.Scheme().ListDirectory(context.Background(), path.Join(s.config.Database, directoryPath))
	if err != nil {
		return nil, xerrors.Errorf("unable to list: %s: %w", directoryPath, err)
	}
	res := make([]string, 0)
	for _, p := range parent.Children {
		if strings.HasPrefix(p.Name, ".") {
			// start with . - means hidden path
			continue
		}
		switch p.Type {
		case scheme.EntryDirectory:
			c, err := s.traverse(path.Join(directoryPath, p.Name))
			if err != nil {
				//nolint:descriptiveerrors
				return nil, xerrors.Errorf("unable to transfer: %s: %w", p.Name, err)
			}
			res = append(res, c...)
		case scheme.EntryTable:
			res = append(res, path.Join(directoryPath, p.Name))
		}
	}
	return res, nil
}

func (s *Storage) canSkipError(err error) bool {
	return ydb.IsOperationErrorSchemeError(err)
}

func validateTableList(params *YdbStorageParams, paths []string) error {
	uniqueFullPaths := make(map[string]bool)
	uniqueRelPaths := make(map[string]bool)
	for _, currTableID := range paths {
		if _, ok := uniqueFullPaths[currTableID]; ok {
			return xerrors.Errorf("found duplicated paths: %s", currTableID)
		}
		uniqueFullPaths[currTableID] = true

		relPath := MakeYDBRelPath(params.UseFullPaths, params.Tables, currTableID)
		if _, ok := uniqueRelPaths[relPath]; ok {
			return xerrors.Errorf("found duplicated relPath: %s, try to turn on UseFullPaths parameter", relPath)
		}
		uniqueRelPaths[relPath] = true
	}
	return nil
}

func (s *Storage) listaAllTablesToTransfer(ctx context.Context) ([]string, error) {
	allTables := []string{}
	if len(s.config.Tables) == 0 {
		result, err := s.traverse("/")
		if err != nil {
			return nil, xerrors.Errorf("Cannot traverse YDB database from root, db: %s, err: %w", s.config.Database, err)
		}
		allTables = result
	} else {
		for _, currPath := range s.config.Tables {
			currPath = strings.TrimSuffix(currPath, "/")
			currFullPath := path.Join(s.config.Database, currPath)

			entry, err := s.db.Scheme().DescribePath(ctx, currFullPath)
			if err != nil {
				return nil, xerrors.Errorf("unable to describe path, path:%s, err:%w", currPath, err)
			}

			if entry.Type == scheme.EntryDirectory {
				subTraverse, err := s.traverse(currPath)
				if err != nil {
					return nil, xerrors.Errorf("Cannot traverse YDB database from root, db: %s, err: %w", s.config.Database, err)
				}
				allTables = append(allTables, subTraverse...)
			} else if entry.Type == scheme.EntryTable {
				allTables = append(allTables, currPath)
			} else {
				return nil, xerrors.Errorf("unknown node type, path:%s, type:%s", currPath, entry.Type.String())
			}
		}
	}

	allTables = yslices.Map(allTables, func(from string) string {
		return strings.TrimLeft(from, "/")
	})
	return allTables, nil
}

func (s *Storage) TableList(includeTableFilter abstract.IncludeTableList) (abstract.TableMap, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Minute*3)
	defer cancel()

	// collect tables entries

	allTables, err := s.listaAllTablesToTransfer(ctx)
	if err != nil {
		return nil, xerrors.Errorf("Failed to list tables that will be transfered: %w", err)
	}

	err = validateTableList(s.config, allTables)
	if err != nil {
		return nil, xerrors.Errorf("vaildation of TableList failed: %w", err)
	}

	tableMap := make(abstract.TableMap)
	for _, tableName := range allTables {
		tablePath := path.Join(s.config.Database, tableName)
		desc, err := describeTable(ctx, s.db, tablePath, options.WithTableStats())
		if err != nil {
			if s.canSkipError(err) {
				logger.Log.Warn("skip table", log.String("table", tablePath), log.Error(err))
				continue
			}
			return nil, xerrors.Errorf("Cannot describe table %s: %w", tablePath, err)
		}
		var tInfo abstract.TableInfo
		if desc.Stats != nil {
			tInfo.EtaRow = desc.Stats.RowsEstimate
		}
		tInfo.Schema = abstract.NewTableSchema(FromYdbSchema(desc.Columns, desc.PrimaryKey))
		tableMap[*abstract.NewTableID("", tableName)] = tInfo
	}
	return model.FilteredMap(tableMap, includeTableFilter), nil
}

func (s *Storage) TableSchema(ctx context.Context, tableID abstract.TableID) (*abstract.TableSchema, error) {
	return tableSchema(ctx, s.db, s.config.Database, tableID)
}

func (s *Storage) LoadTable(ctx context.Context, tableDescr abstract.TableDescription, pusher abstract.Pusher) error {
	st := util.GetTimestampFromContextOrNow(ctx)

	tablePath := s.makeTablePath(tableDescr.Schema, tableDescr.Name)
	partID := tableDescr.PartID()

	var res result.StreamResult
	var schema *abstract.TableSchema

	err := s.db.Table().Do(ctx, func(ctx context.Context, session table.Session) (err error) {
		readTableOptions := []options.ReadTableOption{options.ReadOrdered()}

		tableDescription, err := session.DescribeTable(ctx, tablePath, options.WithShardKeyBounds())
		if err != nil {
			return xerrors.Errorf("unable to describe table: %w", err)
		}
		if s.config.IsSnapshotSharded {
			keyRange := tableDescription.KeyRanges[tableDescr.Offset]
			readTableOptions = append(readTableOptions, options.ReadKeyRange(keyRange))
		}

		tableColumns, err := filterYdbTableColumns(s.config.TableColumnsFilter, tableDescription)
		if err != nil {
			return xerrors.Errorf("unable to filter table columns: %w", err)
		}
		for _, column := range tableColumns {
			readTableOptions = append(readTableOptions, options.ReadColumn(column.Name))
		}

		if filter := tableDescr.Filter; filter != "" {
			from, to, err := s.filterToKeyRange(ctx, filter, tableDescription)
			if err != nil {
				return xerrors.Errorf("error resolving key filter for table %s: %w", tableDescr.Name, err)
			}
			if from != nil {
				readTableOptions = append(readTableOptions, options.ReadGreater(from))
			}
			if to != nil {
				readTableOptions = append(readTableOptions, options.ReadLessOrEqual(to))
			}
		}
		res, err = session.StreamReadTable(ctx, tablePath, readTableOptions...)
		if err != nil {
			return xerrors.Errorf("unable to read table: %w", err)
		}
		schema = abstract.NewTableSchema(FromYdbSchema(tableColumns, tableDescription.PrimaryKey))
		return nil
	})

	if err != nil {
		if s.canSkipError(err) {
			logger.Log.Warn("skip load table", log.String("table", tablePath), log.Error(err))
			return nil
		}
		//nolint:descriptiveerrors
		return err
	}

	cols := make([]string, len(schema.Columns()))
	for i, c := range schema.Columns() {
		cols[i] = c.ColumnName
	}
	totalIdx := uint64(0)
	wrapAroundIdx := uint64(0)
	changes := make([]abstract.ChangeItem, 0)

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			scannerValues := make([]scanner, len(schema.Columns()))
			scannerAttrs := make([]named.Value, len(schema.Columns()))
			for i := range schema.Columns() {
				scannerValues[i] = scanner{
					dataType:     schema.Columns()[i].DataType,
					originalType: schema.Columns()[i].OriginalType,
					resultVal:    nil,
				}
				scannerAttrs[i] = named.Optional(schema.Columns()[i].ColumnName, &scannerValues[i])
			}
			if err := res.ScanNamed(scannerAttrs...); err != nil {
				return xerrors.Errorf("unable to scan table rows: %w", err)
			}

			vals := make([]interface{}, len(schema.Columns()))
			for i := range schema.Columns() {
				vals[i] = scannerValues[i].resultVal
			}

			changes = append(changes, abstract.ChangeItem{
				CommitTime:   uint64(st.UnixNano()),
				Kind:         abstract.InsertKind,
				Table:        tableDescr.Name,
				ColumnNames:  cols,
				ColumnValues: vals,
				TableSchema:  schema,
				PartID:       partID,
				ID:           0,
				LSN:          0,
				Counter:      0,
				Schema:       "",
				OldKeys:      abstract.EmptyOldKeys(),
				TxID:         "",
				Query:        "",
				Size:         abstract.RawEventSize(util.DeepSizeof(vals)),
			})
			s.metrics.ChangeItems.Inc()
			s.metrics.Size.Add(int64(changes[len(changes)-1].Size.Read))
			if wrapAroundIdx == 10000 {
				if err := pusher(changes); err != nil {
					return xerrors.Errorf("unable to push: %w", err)
				}
				changes = make([]abstract.ChangeItem, 0)
				wrapAroundIdx = 0
			}
			totalIdx++
			wrapAroundIdx++
		}
	}
	if res.Err() != nil {
		return xerrors.Errorf("stream read table error: %w", res.Err())
	}

	if wrapAroundIdx != 0 {
		if err := pusher(changes); err != nil {
			return xerrors.Errorf("unable to push: %w", err)
		}
	}
	logger.Log.Info("Sink done uploading table", log.String("fqtn", tableDescr.Fqtn()))
	return nil
}

func Fqtn(tid abstract.TableID) string {
	if tid.Namespace == "" {
		// for YDS / LB schema is empty, this lead to leading _ in name
		return tid.Name
	}
	return tid.Namespace + "_" + tid.Name
}

func (s *Storage) EstimateTableRowsCount(tid abstract.TableID) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tablePath := path.Join(s.config.Database, Fqtn(tid))
	var desc options.Description

	err := s.db.Table().Do(ctx, func(ctx context.Context, session table.Session) (err error) {
		desc, err = session.DescribeTable(ctx, tablePath, options.WithTableStats())
		if err != nil {
			return xerrors.Errorf("unable to descibe table: %w", err)
		}
		return nil
	})

	if err != nil {
		return 0, xerrors.Errorf("unable to descirbe table: %w", err)
	}
	return desc.Stats.RowsEstimate, nil
}

func (s *Storage) ExactTableRowsCount(tid abstract.TableID) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	query := fmt.Sprintf("SELECT count(*) as count FROM `%s`", Fqtn(tid))
	logger.Log.Infof("get exact count: %s", query)
	readTx := table.TxControl(
		table.BeginTx(
			table.WithOnlineReadOnly(),
		),
		table.CommitTx(),
	)

	var res result.Result
	err := s.db.Table().Do(ctx, func(ctx context.Context, session table.Session) (err error) {
		_, res, err = session.Execute(ctx, readTx, query,
			table.NewQueryParameters(),
		)
		if err != nil {
			return xerrors.Errorf("unable to execute count: %w", err)
		}
		return nil
	})

	if err != nil {
		return 0, xerrors.Errorf("unable to descirbe table: %w", err)
	}

	var count uint64
	for res.NextResultSet(ctx, "count") {
		for res.NextRow() {
			err = res.Scan(&count)
			if err != nil {
				return 0, xerrors.Errorf("unable to scan res: %w", err)
			}
		}
	}
	if err = res.Err(); err != nil {
		return 0, xerrors.Errorf("count res error: %w", err)
	}
	return count, nil
}

func (s *Storage) TableExists(tid abstract.TableID) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tablePath := path.Join(s.config.Database, Fqtn(tid))
	logger.Log.Infof("check exists: %v at %s", tid, tablePath)
	err := s.db.Table().Do(ctx, func(ctx context.Context, session table.Session) (err error) {
		_, err = session.DescribeTable(ctx, tablePath)
		if err != nil {
			return xerrors.Errorf("unable to descibe table: %w", err)
		}
		return nil
	})
	if err != nil {
		return false, nil
	}
	return true, nil
}

type scanner struct {
	dataType     string
	originalType string
	resultVal    interface{}
}

func (s *scanner) UnmarshalYDB(raw types.RawValue) error {
	if raw.IsOptional() {
		raw.Unwrap()
	}
	if raw.IsNull() {
		s.resultVal = nil
		return nil
	}
	if s.originalType == "ydb:Decimal" {
		decimalVal := raw.UnwrapDecimal()
		s.resultVal = decimalVal.String()
	} else if s.originalType == "ydb:Json" || s.originalType == "ydb:JsonDocument" {
		var valBytes []byte
		if s.originalType == "ydb:Json" {
			valBytes = raw.JSON()
		} else {
			valBytes = raw.JSONDocument()
		}
		valDecoded, err := jsonx.NewValueDecoder(jsonx.NewDefaultDecoder(bytes.NewReader(valBytes))).Decode()
		if err != nil {
			return xerrors.Errorf("unable to unmarshal JSON '%s': %w", string(valBytes), err)
		}
		s.resultVal = valDecoded
	} else if s.originalType == "ydb:Yson" {
		valBytes := raw.YSON()
		var unmarshalled interface{}
		if len(valBytes) > 0 {
			if err := yson.Unmarshal(valBytes, &unmarshalled); err != nil {
				return xerrors.Errorf("unable to unmarshal: %w", err)
			}
		}
		s.resultVal = unmarshalled
	} else if s.originalType == "ydb:Uuid" {
		s.resultVal = raw.UUIDTyped().String()
	} else {
		switch schema.Type(s.dataType) {
		case schema.TypeDate:
			s.resultVal = raw.Date().UTC()
		case schema.TypeTimestamp:
			s.resultVal = raw.Timestamp().UTC()
		case schema.TypeDatetime:
			s.resultVal = raw.Datetime().UTC()
		case schema.TypeInterval:
			s.resultVal = raw.Interval()
		default:
			s.resultVal = raw.Any()
		}
	}
	return nil
}
