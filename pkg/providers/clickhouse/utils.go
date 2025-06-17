//go:build !disable_clickhouse_provider

package clickhouse

import (
	"database/sql"
	"fmt"
	"net"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/connection/clickhouse"
)

func InitValuesForScan(rows *sql.Rows) ([]interface{}, error) {
	resColtypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, xerrors.Errorf("unable to parse select table rows result: %w", err)
	}

	resValues := make([]interface{}, len(resColtypes))
	for i, coltype := range resColtypes {
		resValues[i] = reflect.New(coltype.ScanType()).Interface()
	}
	return resValues, nil
}

func marshalField(value interface{}, origType string) (interface{}, uint64) {
	switch val := value.(type) {
	// TODO support complex types
	// Decimal presents as *int32
	// DateTime64 - as *time.Time
	// Array - as *[]valType
	// Tuple - as *[]interface{}
	// Enum - as *string
	case *sql.NullInt32:
		if val.Valid {
			return val.Int32, uint64(unsafe.Sizeof(val.Int32))
		} else {
			return nil, 0
		}
	case *sql.NullInt64:
		if val.Valid {
			return val.Int64, uint64(unsafe.Sizeof(val.Int64))
		} else {
			return nil, 0
		}
	case *sql.NullBool:
		if val.Valid {
			return val.Bool, uint64(unsafe.Sizeof(val.Bool))
		} else {
			return nil, 0
		}
	case *sql.NullFloat64:
		if val.Valid {
			return val.Float64, uint64(unsafe.Sizeof(val.Float64))
		} else {
			return nil, 0
		}
	case *sql.NullString:
		if val.Valid {
			return val.String, uint64(len(val.String))
		} else {
			return nil, 0
		}
	case *sql.NullTime:
		if val.Valid {
			return marshalField(&val.Time, origType)
		} else {
			return nil, 0
		}
	case *sql.RawBytes:
		if *val == nil {
			return nil, 0
		}
		valueBytesCopy := make([]byte, len(*val))
		copy(valueBytesCopy, []byte(*val))
		return string(valueBytesCopy), uint64(len(*val))
	case *time.Time:
		if val == nil {
			return nil, 0
		}
		sz := uint64(unsafe.Sizeof(*val))
		// CH Date special thing that not store any timezone and always UTC based value, yet has no time part.
		// we need to make preserve UTC-based base date at 00:00:00 time
		if strings.Contains(origType, "Date") && !strings.Contains(origType, "DateTime") {
			_, offset := val.Zone()
			return val.UTC().Add(time.Second * time.Duration(offset)), sz
		}
		// Fix to support clickhouse-go/v1 driver behaviour
		if val.IsZero() {
			return time.Unix(0, 0), sz
		}
		return val.UTC(), sz
	case *int:
		if val == nil {
			return nil, 0
		}
		return *val, uint64(unsafe.Sizeof(*val))
	case *int8:
		if val == nil {
			return nil, 0
		}
		return *val, uint64(unsafe.Sizeof(*val))
	case *int16:
		if val == nil {
			return nil, 0
		}
		return *val, uint64(unsafe.Sizeof(*val))
	case *int32:
		if val == nil {
			return nil, 0
		}
		return *val, uint64(unsafe.Sizeof(*val))
	case *int64:
		if val == nil {
			return nil, 0
		}
		return *val, uint64(unsafe.Sizeof(*val))
	case *uint:
		if val == nil {
			return nil, 0
		}
		return *val, uint64(unsafe.Sizeof(*val))
	case *uint8:
		if val == nil {
			return nil, 0
		}
		return *val, uint64(unsafe.Sizeof(*val))
	case *uint16:
		if val == nil {
			return nil, 0
		}
		return *val, uint64(unsafe.Sizeof(*val))
	case *uint32:
		if val == nil {
			return nil, 0
		}
		return *val, uint64(unsafe.Sizeof(*val))
	case *uint64:
		if val == nil {
			return nil, 0
		}
		return *val, uint64(unsafe.Sizeof(*val))
	case *float32:
		if val == nil {
			return nil, 0
		}
		return *val, uint64(unsafe.Sizeof(*val))
	case *float64:
		if val == nil {
			return nil, 0
		}
		return *val, uint64(unsafe.Sizeof(*val))
	case []uint8:
		if val == nil {
			return nil, 0
		}
		if strings.Contains(origType, "Array(UInt8)") {
			return val, uint64(len(val))
		}
		return string(val), uint64(len(val))
	case *string:
		if val == nil {
			return nil, 0
		}
		return *val, uint64(len(*val))
	case *net.IP:
		if val == nil {
			return nil, 0
		}
		str := val.String()
		return str, uint64(len(str))
	case **string:
		if val == nil || *val == nil {
			return nil, 0
		}
		return **val, uint64(len(**val))
	case *decimal.Decimal:
		if val == nil {
			return nil, 0
		}
		v := val.String()
		return v, uint64(len(v))
	case []*decimal.Decimal:
		var res []*string
		var l uint64
		for _, v := range val {
			if v == nil {
				res = append(res, nil)
			} else {
				t := v.String()
				res = append(res, &t)
				l += uint64(len(t)) // Incorrect, true bytes should be inferred from CH type
			}
		}
		return res, l
	case *uuid.UUID:
		if val == nil {
			return nil, 0
		}
		v := val.String()
		return v, uint64(len(v))
	case []*uuid.UUID:
		var res []*string
		var l uint64
		for _, v := range val {
			if v == nil {
				res = append(res, nil)
			} else {
				t := v.String()
				res = append(res, &t)
				l += uint64(len(v))
			}
		}
		return res, l
	default:
		if val != nil {
			v := reflect.ValueOf(val)
			if v.Kind() == reflect.Slice {
				return copySlice(val)
			}
			if v.Kind() == reflect.Ptr {
				if v.IsNil() {
					return nil, 0
				}
				e := v.Elem().Interface()
				return marshalField(e, origType)
			}
		}
		// we cannot use unsafe.Sizeof here bcs each value of interface type is a tuple of two pointers: a pointer to the enclosed value's real type and the value itself
		// thus, we will always get 16 bytes as the size, regardless of what type is actually hidden under the interface
		return val, uint64(reflect.TypeOf(val).Size())
	}
}

func copySlice(src interface{}) (interface{}, uint64) {
	t := reflect.TypeOf(src)
	v := reflect.ValueOf(src)
	r := reflect.MakeSlice(t, v.Len(), v.Len())
	reflect.Copy(r, v)

	valuesSize := uint64(0)
	valuesCount := v.Len()
	if eTyp := t.Elem(); eTyp.Kind() != reflect.String {
		valuesSize = uint64(valuesCount) * uint64(eTyp.Size())
	} else {
		for i := 0; i < valuesCount; i++ {
			valuesSize += uint64(v.Index(i).Len())
		}
	}
	return r.Interface(), uint64(t.Size()) + valuesSize
}

func MarshalFields(vals []interface{}, schema abstract.TableColumns) ([]interface{}, uint64) {
	rowColsIndex := make(map[string]int)
	for i, col := range schema {
		rowColsIndex[col.ColumnName] = i
	}

	resVals := make([]interface{}, len(schema))
	size := uint64(unsafe.Sizeof(vals))

	for i, col := range schema {
		rowIdx, ok := rowColsIndex[col.ColumnName]
		if !ok {
			continue
		}
		v, vSize := marshalField(vals[rowIdx], col.OriginalType)
		resVals[i] = v
		size += vSize
	}
	return resVals, size
}

func IsColVirtual(colSchema abstract.ColSchema) bool {
	return strings.HasPrefix(colSchema.Expression, "MATERIALIZED:") || strings.HasPrefix(colSchema.Expression, "ALIAS:")
}

func ColumnShouldBeSelected(col abstract.ColSchema, isHomo bool) bool {
	return (!isHomo) || (isHomo && !IsColVirtual(col))
}

func ShardsToString(shards map[string][]*clickhouse.Host) string {
	shardsToString := make(map[string]string, len(shards))
	for shard, hosts := range shards {
		shardsToString[shard] = HostsToString(hosts)
	}
	return fmt.Sprintf("%v", shardsToString)
}

func HostsToString(hosts []*clickhouse.Host) string {
	hostsToString := make([]string, 0, len(hosts))
	for _, host := range hosts {
		hostsToString = append(hostsToString, host.String())
	}
	return fmt.Sprintf("%v", hostsToString)
}
