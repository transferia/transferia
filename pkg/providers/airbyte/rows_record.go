//go:build !disable_airbyte_provider

package airbyte

import (
	"strconv"
	"time"

	"github.com/araddon/dateparse"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/base/adapter"
	"github.com/transferia/transferia/pkg/base/events"
	"github.com/transferia/transferia/pkg/base/types"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/yt/go/schema"
)

type RowsRecord struct {
	Stream      *AirbyteStream
	Record      *Record
	cols        []string
	TableSchema *abstract.TableSchema
	colToIndex  map[string]int
	rowIndex    int
}

func (s *RowsRecord) ToOldChangeItem() (*abstract.ChangeItem, error) {
	if err := s.Record.LazyParse(); err != nil {
		return nil, xerrors.Errorf("unable to parse record data: %w", err)
	}
	values := make([]interface{}, len(s.cols))
	var errs util.Errors
	for i, col := range s.cols {
		if col == RecordIndexCol.ColumnName {
			values[i] = int64(s.rowIndex)
			continue
		}
		restoredValue, err := restore(s.TableSchema.Columns()[s.colToIndex[col]], s.Record.ParsedData[col])
		if err != nil {
			errs = append(errs, xerrors.Errorf("restore %s value faile: %w", col, err))
		}
		values[i] = restoredValue
	}
	if len(errs) > 0 {
		return nil, xerrors.Errorf("unable to convert change: %w", errs)
	}
	return &abstract.ChangeItem{
		ID:           uint32(s.Record.EmittedAt),
		LSN:          0,
		CommitTime:   uint64(time.Unix(s.Record.EmittedAt/1000, s.Record.EmittedAt*int64(time.Millisecond)).UnixNano()),
		Counter:      0,
		Kind:         abstract.InsertKind,
		Schema:       s.Stream.Namespace,
		Table:        s.Stream.Name,
		PartID:       "",
		ColumnNames:  s.cols,
		ColumnValues: values,
		TableSchema:  s.TableSchema,
		OldKeys:      *new(abstract.OldKeysType),
		TxID:         "",
		Query:        "",
		Size:         abstract.EmptyEventSize(),
	}, nil
}

func restore(col abstract.ColSchema, val interface{}) (interface{}, error) {
	switch schema.Type(col.DataType) {
	case schema.TypeInt64:
		switch v := val.(type) {
		case string:
			parsedInt, err := strconv.Atoi(v)
			if err == nil {
				return parsedInt, nil
			}
			// airbyte may present internal time as string based time in record, but as unix int64 in data schema
			// ¯\_(ツ)_/¯, so let's try to parse this crappy data as date
			t, err := dateparse.ParseAny(v)
			if err != nil {
				return nil, xerrors.Errorf("unable to parse date time %v: %w", v, err)
			}
			return t.Unix(), nil
		}
	default:
	}
	return abstract.Restore(col, val), nil
}

func (s *RowsRecord) Table() base.Table {
	return adapter.NewTableFromLegacy(s.TableSchema, s.Stream.TableID())
}

func (s *RowsRecord) NewValuesCount() int {
	return len(s.TableSchema.Columns())
}

func (s *RowsRecord) NewValue(i int) (base.Value, error) {
	if err := s.Record.LazyParse(); err != nil {
		return nil, xerrors.Errorf("unable to parse record data: %w", err)
	}
	for _, t := range s.Stream.JSONSchema.Properties[s.cols[i]].Type {
		switch t {
		case "date-time":
			switch v := s.Record.ParsedData[s.cols[i]].(type) {
			case string:
				t, err := dateparse.ParseAny(v)
				if err != nil {
					return nil, xerrors.Errorf("unable to parse date time %v: %w", v, err)
				}
				return types.NewDefaultDateTimeValue(&t, nil), nil
			}
		case "integer":
			switch v := s.Record.ParsedData[s.cols[i]].(type) {
			case int32:
				return types.NewDefaultInt32Value(&v, nil), nil
			}
		case "number":
			switch v := s.Record.ParsedData[s.cols[i]].(type) {
			case float64:
				return types.NewDefaultDoubleValue(&v, nil), nil
			}
		case "string":
			switch v := s.Record.ParsedData[s.cols[i]].(type) {
			case string:
				return types.NewDefaultStringValue(&v, nil), nil
			}
		case "boolean":
			switch v := s.Record.ParsedData[s.cols[i]].(type) {
			case bool:
				return types.NewDefaultBoolValue(&v, nil), nil
			}
		}
	}
	return nil, xerrors.Errorf(
		"unknown value type: %v(%T)",
		s.Stream.JSONSchema.Properties[s.cols[i]].Type,
		s.Record.ParsedData[s.cols[i]],
	)
}

func NewStreamRecord(
	stream *AirbyteStream,
	cols []string,
	record *Record,
	tableSchema *abstract.TableSchema,
	colToIndex map[string]int,
	rowIndex int,
) events.InsertEvent {
	return &RowsRecord{
		Stream:      stream,
		Record:      record,
		cols:        cols,
		TableSchema: tableSchema,
		colToIndex:  colToIndex,
		rowIndex:    rowIndex,
	}
}
