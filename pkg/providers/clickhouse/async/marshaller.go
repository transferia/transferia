package async

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/errors/coded"
	error_codes "github.com/transferia/transferia/pkg/errors/codes"
	ch_db_model "github.com/transferia/transferia/pkg/providers/clickhouse/async/model/db"
	"github.com/transferia/transferia/pkg/providers/clickhouse/columntypes"
)

type marshallingError struct {
	err  error
	code coded.Code
}

func (e *marshallingError) IsMarshallingError() {}

func (e *marshallingError) Error() string {
	return e.err.Error()
}

func (e *marshallingError) Code() coded.Code {
	return e.code
}

func colMarshallingError(code coded.Code, name string, val any, text string) *marshallingError {
	return &marshallingError{
		err: xerrors.Errorf("error marshalling column %s value %v of type %T as clickhouse value: %s",
			name, val, val, text),
		code: code,
	}
}

func genericMarshallingError(err error) *marshallingError {
	return &marshallingError{
		err:  err,
		code: error_codes.DataValueError,
	}
}

// marshalChangeItemInto fills dst (pre-allocated by caller) without allocating a new slice.
// dst must have len == len(row.ColumnValues).
func marshalChangeItemInto(dst []any, row abstract.ChangeItem, schema map[string]abstract.ColSchema, cols columntypes.TypeMapping) (retErr error) {
	defer func() {
		// Restore() may panic now
		val := recover()
		if val == nil {
			return
		}
		var err error
		if e, ok := val.(error); ok {
			err = e
		} else {
			err = xerrors.Errorf("marshalling panic: %v", val)
		}
		retErr = genericMarshallingError(err)
	}()
	for i, col := range row.ColumnNames {
		rowVal := row.ColumnValues[i]
		colType := cols[col]
		colSch := schema[col]
		if rowVal == nil {
			dst[i] = rowVal
		} else if colType.IsArray && colSch.DataType == "any" {
			dst[i] = rowVal
		} else if colType.IsString {
			switch rowValDowncasted := rowVal.(type) {
			case string, byte:
				dst[i] = rowVal
			case []byte:
				dst[i] = string(rowValDowncasted)
			case []any:
				if len(rowValDowncasted) == 0 {
					dst[i] = "[]"
				} else {
					b, _ := json.Marshal(rowValDowncasted)
					dst[i] = string(b)
				}
			default:
				b, _ := json.Marshal(rowVal)
				dst[i] = string(b)
			}
		} else if colType.IsDecimal {
			var val decimal.Decimal
			var decErr error
			switch v := rowVal.(type) {
			case string:
				val, decErr = decimal.NewFromString(v)
			case []byte:
				val, decErr = decimal.NewFromString(string(v))
			case json.Number:
				val, decErr = decimal.NewFromString(v.String())
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
				val, decErr = decimal.NewFromString(fmt.Sprintf("%d", v))
			case float32:
				val = decimal.NewFromFloat32(v)
			case float64:
				val = decimal.NewFromFloat(v)
			default:
				//nolint:descriptiveerrors
				return colMarshallingError(error_codes.UnsupportedConversion, col, v, "unknown type for decimal column")
			}
			if decErr != nil {
				//nolint:descriptiveerrors
				return colMarshallingError(error_codes.DataValueError, col, rowVal, fmt.Sprintf("error converting to decimal: %s", decErr))
			}
			dst[i] = val
		} else {
			dst[i] = columntypes.Restore(colSch, rowVal)
			if v, ok := dst[i].(string); ok && (colType.IsDateTime64 || colType.IsDateTime || colType.IsDate) {
				// FIXME: market hack/workaround, should rethink and rewrite this part before public release
				// If date/datetime is inserted as raw string,
				// it is interpreted in timezone specified in column settings or in server timezone
				// CH date/datetimes cannot store values before 1970-01-01T00:00:00Z (UTC)
				// So an attempt to insert string like 1970-01-01 00:00:00 to Clickhouse with Msk server timezone
				// fails as this time is 3 hours before 1970-01-01T00:00:00Z
				// Since usually dates in 1970 means default empty dates, replace such values with zero date
				if strings.HasPrefix(v, "1970-01-01") {
					dst[i] = time.Unix(0, 0).UTC()
					continue
				}
				if colType.IsDateTime {
					dst[i] = strings.Replace(v, "T", " ", 1)
				}
			}
		}
	}
	return nil
}

type chV2Marshaller struct {
	schema map[string]abstract.ColSchema
	cols   columntypes.TypeMapping
	buf    []any
}

func (m *chV2Marshaller) marshal(row abstract.ChangeItem) ([]any, error) {
	n := len(row.ColumnValues)
	if cap(m.buf) < n {
		m.buf = make([]any, n)
	}
	m.buf = m.buf[:n]
	if err := marshalChangeItemInto(m.buf, row, m.schema, m.cols); err != nil {
		return nil, err
	}
	return m.buf, nil
}

func NewCHV2Marshaller(schema []abstract.ColSchema, cols columntypes.TypeMapping) ch_db_model.ChangeItemMarshaller {
	sch := make(map[string]abstract.ColSchema)
	for _, col := range schema {
		sch[col.ColumnName] = col
	}
	m := &chV2Marshaller{
		schema: sch,
		cols:   cols,
		buf:    nil,
	}
	return m.marshal
}
