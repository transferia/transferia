//go:build !disable_clickhouse_provider

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
	"github.com/transferia/transferia/pkg/providers"
	"github.com/transferia/transferia/pkg/providers/clickhouse/async/model/db"
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
		code: providers.DataValueError,
	}
}

func marshalChangeItem(row abstract.ChangeItem, schema map[string]abstract.ColSchema, cols columntypes.TypeMapping) ([]any, error) {
	var err error
	defer func() {
		// Restore() may panic now
		val := recover()
		if val == nil {
			return
		}
		if e, ok := val.(error); ok {
			err = e
		} else {
			err = xerrors.Errorf("marshalling panic: %v", val)
		}
		err = genericMarshallingError(err)
	}()
	vals := make([]interface{}, len(row.ColumnValues))
	for i, col := range row.ColumnNames {
		rowVal := row.ColumnValues[i]
		colType := cols[col]
		colSch := schema[col]
		if rowVal == nil {
			vals[i] = rowVal
		} else if colType.IsArray && strings.ToLower(colSch.DataType) == "any" {
			vals[i] = rowVal
		} else if colType.IsString {
			switch rowValDowncasted := rowVal.(type) {
			case string, byte:
				vals[i] = rowVal
			case []byte:
				vals[i] = string(rowValDowncasted)
			default:
				b, _ := json.Marshal(rowVal)
				vals[i] = string(b)
			}
		} else if colType.IsDecimal {
			var val decimal.Decimal
			switch v := rowVal.(type) {
			case string:
				val, err = decimal.NewFromString(v)
			case []byte:
				val, err = decimal.NewFromString(string(v))
			case json.Number:
				val, err = decimal.NewFromString(v.String())
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
				val, err = decimal.NewFromString(fmt.Sprintf("%d", v))
			case float32:
				val = decimal.NewFromFloat32(v)
			case float64:
				val = decimal.NewFromFloat(v)
			default:
				//nolint:descriptiveerrors
				return nil, colMarshallingError(providers.UnsupportedConversion, col, v, "unknown type for decimal column")
			}
			if err != nil {
				//nolint:descriptiveerrors
				return nil, colMarshallingError(providers.DataValueError, col, rowVal, fmt.Sprintf("error converting to decimal: %s", err))
			}
			vals[i] = val
		} else {
			vals[i] = columntypes.Restore(colSch, rowVal)
			if v, ok := vals[i].(string); ok && (colType.IsDateTime64 || colType.IsDateTime || colType.IsDate) {
				// FIXME: market hack/workaround, should rethink and rewrite this part before public release
				// If date/datetime is inserted as raw string,
				// it is interpreted in timezone specified in column settings or in server timezone
				// CH date/datetimes cannot store values before 1970-01-01T00:00:00Z (UTC)
				// So an attempt to insert string like 1970-01-01 00:00:00 to Clickhouse with Msk server timezone
				// fails as this time is 3 hours before 1970-01-01T00:00:00Z
				// Since usually dates in 1970 means default empty dates, replace such values with zero date
				if strings.HasPrefix(v, "1970-01-01") {
					vals[i] = time.Unix(0, 0).UTC()
					continue
				}
				if colType.IsDateTime {
					vals[i] = strings.Replace(v, "T", " ", 1)
				}
			}
		}
	}
	//nolint:descriptiveerrors
	return vals, err
}

func NewCHV2Marshaller(schema []abstract.ColSchema, cols columntypes.TypeMapping) db.ChangeItemMarshaller {
	sch := make(map[string]abstract.ColSchema)
	for _, col := range schema {
		sch[col.ColumnName] = col
	}
	return func(item abstract.ChangeItem) ([]any, error) {
		return marshalChangeItem(item, sch, cols)
	}
}
