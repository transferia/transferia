package ydb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/jsonx"
	ydb_query "github.com/ydb-platform/ydb-go-sdk/v3/query"
	ydb_options "github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	ydb_table_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.ytsaurus.tech/library/go/core/log"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yson"
)

// queryValueToGoNative converts a ydb_table_types.Value obtained from the Query Service
// into a Go-native value following the same conventions as scanner.UnmarshalYDB.
func queryValueToGoNative(v ydb_table_types.Value, dataType, originalType string) (interface{}, error) {
	if ydb_table_types.IsNull(v) {
		return nil, nil
	}
	v = ydb_table_types.Unwrap(v)

	switch originalType {
	case "ydb:Decimal":
		s, err := ydb_table_types.ToDecimal(v)
		if err != nil {
			return nil, xerrors.Errorf("unable to cast Decimal to string: %w", err)
		}
		return s.String(), nil
	case "ydb:Json", "ydb:JsonDocument":
		var s string
		if err := ydb_table_types.CastTo(v, &s); err != nil {
			return nil, xerrors.Errorf("unable to cast JSON to string: %w", err)
		}
		valDecoded, err := jsonx.NewValueDecoder(jsonx.NewDefaultDecoder(bytes.NewReader([]byte(s)))).Decode()
		if err != nil {
			return nil, xerrors.Errorf("unable to unmarshal JSON '%s': %w", s, err)
		}
		return valDecoded, nil
	case "ydb:Yson":
		var b []byte
		if err := ydb_table_types.CastTo(v, &b); err != nil {
			return nil, xerrors.Errorf("unable to cast YSON to bytes: %w", err)
		}
		var unmarshalled interface{}
		if len(b) > 0 {
			if err := yson.Unmarshal(b, &unmarshalled); err != nil {
				return nil, xerrors.Errorf("unable to unmarshal YSON: %w", err)
			}
		}
		return unmarshalled, nil
	case "ydb:Uuid":
		var u uuid.UUID
		if err := ydb_table_types.CastTo(v, &u); err != nil {
			return nil, xerrors.Errorf("unable to cast UUID: %w", err)
		}
		return u.String(), nil
	}

	switch ytschema.Type(dataType) {
	case ytschema.TypeDate, ytschema.TypeDatetime, ytschema.TypeTimestamp:
		var t time.Time
		if err := ydb_table_types.CastTo(v, &t); err != nil {
			return nil, xerrors.Errorf("unable to cast time value (type %s): %w", dataType, err)
		}
		return t.UTC(), nil
	case ytschema.TypeInterval:
		var d time.Duration
		if err := ydb_table_types.CastTo(v, &d); err != nil {
			return nil, xerrors.Errorf("unable to cast interval: %w", err)
		}
		return d, nil
	}

	switch originalType {
	case "ydb:Bool":
		var r bool
		if err := ydb_table_types.CastTo(v, &r); err != nil {
			return nil, xerrors.Errorf("unable to cast Bool: %w", err)
		}
		return r, nil
	case "ydb:Int8":
		var r int8
		if err := ydb_table_types.CastTo(v, &r); err != nil {
			return nil, xerrors.Errorf("unable to cast Int8: %w", err)
		}
		return r, nil
	case "ydb:Int16":
		var r int16
		if err := ydb_table_types.CastTo(v, &r); err != nil {
			return nil, xerrors.Errorf("unable to cast Int16: %w", err)
		}
		return r, nil
	case "ydb:Int32":
		var r int32
		if err := ydb_table_types.CastTo(v, &r); err != nil {
			return nil, xerrors.Errorf("unable to cast Int32: %w", err)
		}
		return r, nil
	case "ydb:Int64":
		var r int64
		if err := ydb_table_types.CastTo(v, &r); err != nil {
			return nil, xerrors.Errorf("unable to cast Int64: %w", err)
		}
		return r, nil
	case "ydb:Uint8":
		var r uint8
		if err := ydb_table_types.CastTo(v, &r); err != nil {
			return nil, xerrors.Errorf("unable to cast Uint8: %w", err)
		}
		return r, nil
	case "ydb:Uint16":
		var r uint16
		if err := ydb_table_types.CastTo(v, &r); err != nil {
			return nil, xerrors.Errorf("unable to cast Uint16: %w", err)
		}
		return r, nil
	case "ydb:Uint32":
		var r uint32
		if err := ydb_table_types.CastTo(v, &r); err != nil {
			return nil, xerrors.Errorf("unable to cast Uint32: %w", err)
		}
		return r, nil
	case "ydb:Uint64":
		var r uint64
		if err := ydb_table_types.CastTo(v, &r); err != nil {
			return nil, xerrors.Errorf("unable to cast Uint64: %w", err)
		}
		return r, nil
	case "ydb:Float":
		var r float32
		if err := ydb_table_types.CastTo(v, &r); err != nil {
			return nil, xerrors.Errorf("unable to cast Float: %w", err)
		}
		return r, nil
	case "ydb:Double":
		var r float64
		if err := ydb_table_types.CastTo(v, &r); err != nil {
			return nil, xerrors.Errorf("unable to cast Double: %w", err)
		}
		return r, nil
	case "ydb:String":
		var r []byte
		if err := ydb_table_types.CastTo(v, &r); err != nil {
			return nil, xerrors.Errorf("unable to cast String (bytes): %w", err)
		}
		return r, nil
	case "ydb:Utf8", "ydb:DyNumber":
		var r string
		if err := ydb_table_types.CastTo(v, &r); err != nil {
			return nil, xerrors.Errorf("unable to cast Utf8/DyNumber: %w", err)
		}
		return r, nil
	default:
		return v, nil
	}
}

func (s *Storage) LoadTableViaQueryService(ctx context.Context, tableDescr abstract.TableDescription, pusher abstract.Pusher) error {
	st := util.GetTimestampFromContextOrNow(ctx)

	tablePath := s.makeTablePath(tableDescr.Schema, tableDescr.Name)
	tableDescription, err := describeTable(ctx, s.db, tablePath)
	if err != nil {
		return xerrors.Errorf("unable to describe table: %w", err)
	}

	tableColumns, err := filterYdbTableColumns(s.config.TableColumnsFilter, *tableDescription)
	if err != nil {
		return xerrors.Errorf("unable to filter table columns: %w", err)
	}

	schema := abstract.NewTableSchema(FromYdbSchema(tableColumns, tableDescription.PrimaryKey))

	selectQuery, err := s.buildSelectQuery(ctx, tableColumns, *tableDescription, tableDescr)
	if err != nil {
		return xerrors.Errorf("unable to build query: %w", err)
	}

	var res ydb_query.Result
	if err := s.db.Query().Do(ctx, func(ctx context.Context, session ydb_query.Session) error {
		res, err = session.Query(ctx, selectQuery)
		if err != nil {
			return xerrors.Errorf("unable to execute query: %w", err)
		}
		return nil
	}, ydb_query.WithIdempotent()); err != nil {
		return xerrors.Errorf("failed to load table: %w", err)
	}

	cols := make([]string, len(schema.Columns()))
	for i, c := range schema.Columns() {
		cols[i] = c.ColumnName
	}

	maxBatchLen := s.config.MaxBatchLenOrDefault()
	maxBatchSize := s.config.MaxBatchSizeOrDefault()

	var batchSize uint64
	batch := make([]abstract.ChangeItem, 0, maxBatchLen)

	for {
		rs, err := res.NextResultSet(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return xerrors.Errorf("unable to get next result set: %w", err)
		}

		for {
			row, err := rs.NextRow(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return xerrors.Errorf("unable to get next row: %w", err)
			}

			rawVals := make([]ydb_table_types.Value, len(schema.Columns()))
			scanDsts := make([]ydb_query.NamedDestination, len(rawVals))
			for i := range schema.Columns() {
				scanDsts[i] = ydb_query.Named(schema.Columns()[i].ColumnName, &rawVals[i])
			}
			if err := row.ScanNamed(scanDsts...); err != nil {
				return xerrors.Errorf("unable to scan row: %w", err)
			}

			vals := make([]interface{}, len(schema.Columns()))
			for i, col := range schema.Columns() {
				vals[i], err = queryValueToGoNative(rawVals[i], col.DataType, col.OriginalType)
				if err != nil {
					return xerrors.Errorf("unable to convert value for column %s: %w", col.ColumnName, err)
				}
			}

			valuesSize := util.DeepSizeof(vals)
			batch = append(batch, abstract.ChangeItem{
				ID:               0,
				LSN:              0,
				CommitTime:       uint64(st.UnixNano()),
				Counter:          0,
				Kind:             abstract.InsertKind,
				Schema:           "",
				Table:            tableDescr.Name,
				PartID:           tableDescr.GeneratePartID(),
				ColumnNames:      cols,
				ColumnValues:     vals,
				TableSchema:      schema,
				OldKeys:          abstract.EmptyOldKeys(),
				Size:             abstract.RawEventSize(valuesSize),
				TxID:             "",
				Query:            "",
				QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: "", PartitionNum: 0, Offset: 0, Index: 0},
			})
			batchSize += valuesSize

			s.metrics.ChangeItems.Inc()
			s.metrics.Size.Add(int64(valuesSize))

			if len(batch) >= maxBatchLen || batchSize >= uint64(maxBatchSize) {
				if err := pusher(batch); err != nil {
					return xerrors.Errorf("unable to push: %w", err)
				}
				batchSize = 0
				batch = make([]abstract.ChangeItem, 0, maxBatchLen)
			}
		}
	}

	if len(batch) > 0 {
		if err := pusher(batch); err != nil {
			return xerrors.Errorf("unable to push: %w", err)
		}
	}
	logger.Log.Info("Sink done uploading table via query service", log.String("fqtn", tableDescr.Fqtn()))
	return nil
}

func (s *Storage) buildSelectQuery(
	ctx context.Context,
	tableColumns []ydb_options.Column,
	tableDescription ydb_options.Description,
	tableDescr abstract.TableDescription) (string, error) {
	quotedCols := make([]string, len(tableColumns))
	for i, col := range tableColumns {
		quotedCols[i] = fmt.Sprintf("`%s`", col.Name)
	}

	selectQuery := fmt.Sprintf("--!syntax_v1\nSELECT %s FROM `%s`", strings.Join(quotedCols, ", "), tableDescr.Name)

	if len(tableDescription.PrimaryKey) == 0 {
		return selectQuery, nil
	}

	if filter := tableDescr.Filter; filter != "" {
		from, to, err := s.filterToKeyRange(ctx, filter, tableDescription)
		if err != nil {
			return "", xerrors.Errorf("error resolving key filter for table %s: %w", tableDescription.Name, err)
		}

		var greater abstract.WhereStatement
		if from != nil {
			greater = abstract.WhereStatement(fmt.Sprintf("`%s` > %s", tableDescription.PrimaryKey[0], from.Yql()))
		}

		var smaller abstract.WhereStatement
		if to != nil {
			smaller = abstract.WhereStatement(fmt.Sprintf("`%s` > %s", tableDescription.PrimaryKey[0], to.Yql()))
			smaller = abstract.NotStatement(smaller)
		}

		selectQuery += fmt.Sprintf(" WHERE %s", abstract.FiltersIntersection(greater, smaller))
	}

	pkCols := make([]string, len(tableDescription.PrimaryKey))
	for i, pk := range tableDescription.PrimaryKey {
		pkCols[i] = fmt.Sprintf("`%s`", pk)
	}
	selectQuery += fmt.Sprintf(" ORDER BY %s", strings.Join(pkCols, ", "))

	return selectQuery, nil
}

func (s *Storage) isSystemTable(path string) bool {
	return strings.HasPrefix(path, ".")
}
