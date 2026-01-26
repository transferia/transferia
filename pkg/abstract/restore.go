package abstract

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	"github.com/gofrs/uuid"
	"github.com/spf13/cast"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util/jsonx"
	"go.ytsaurus.tech/yt/go/schema"
)

func Restore(column ColSchema, value interface{}) interface{} {
	if value == nil {
		return value
	}

	switch v := value.(type) {
	case *time.Time:
		if v == nil {
			return v
		}
		return Restore(column, *v)
	case uuid.UUID:
		return v.String()
	case time.Time:
		switch column.DataType {
		case string(schema.TypeDate), string(schema.TypeDatetime), string(schema.TypeTimestamp):
			return v
		case "int64":
			return -v.UnixNano()
		case "uint64":
			return v.UnixNano()
		case "int32":
			return -v.Unix()
		case "uint32":
			return v.Unix()
		case "utf8", "string", "any":
			return v.Format(time.RFC3339Nano)
		}
	case []any:
		result := make([]any, len(v))
		for i := range v {
			result[i] = Restore(column, v[i])
		}
		return result
	}

	switch column.DataType {
	case string(schema.TypeInterval):
		switch v := value.(type) {
		case time.Duration:
			return v
		case float64:
			return time.Duration(int64(v))
		case int64:
			return time.Duration(v)
		case json.Number:
			if s, err := strconv.ParseInt(v.String(), 10, 64); err == nil {
				return Restore(column, s)
			}
			return int64(0)
		default:
			// TODO: Return actual error instead of panic
			// for now it works
			panic(fmt.Sprintf("impossible value: %T %v", v, v))
		}
	case string(schema.TypeDate), string(schema.TypeDatetime), string(schema.TypeTimestamp):
		switch v := value.(type) {
		case string:
			t, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				if tt, err := dateparse.ParseAny(v); err != nil {
					panic(err) // this should be impossible
				} else {
					return tt
				}
			}
			return t
		case time.Time:
			return v
		case int64:
			// this is for backward compatibility, some of our transfer copy data from mysql to LB, see: TM-4015
			// remove in march
			switch schema.Type(column.DataType) {
			case schema.TypeDate:
				return schema.Date(v).Time().UTC()
			case schema.TypeTimestamp:
				return schema.Timestamp(v).Time().UTC()
			case schema.TypeDatetime:
				return schema.Datetime(v).Time().UTC()
			}
		case json.Number:
			if s, err := strconv.ParseInt(v.String(), 10, 64); err == nil {
				return Restore(column, s)
			}
			return int64(0)
		default:
			// TODO: Return actual error instead of panic
			// for now it works
			panic(fmt.Sprintf("impossible value: %T %v", v, v))
		}
	case "int64":
		return cast.ToInt64(maybeStringToNumeric(value))
	case "int32":
		return cast.ToInt32(maybeStringToNumeric(value))
	case "int16":
		return cast.ToInt16(maybeStringToNumeric(value))
	case "int8":
		return cast.ToInt8(maybeStringToNumeric(value))
	case "uint64":
		return cast.ToUint64(maybeStringToNumeric(value))
	case "uint32":
		return cast.ToUint32(maybeStringToNumeric(value))
	case "uint16":
		return cast.ToUint16(maybeStringToNumeric(value))
	case "uint8":
		return cast.ToUint8(maybeStringToNumeric(value))
	case "double":
		switch v := value.(type) {
		case float32:
			return float64(v)
		case float64:
			return value
		case string:
			if v == "" {
				return nil
			}
			if s, err := strconv.ParseFloat(v, 64); err == nil {
				return s
			}
			return nil
		case json.Number:
			return v
		default:
			return nil
		}
	case "boolean":
		return value
	case "string", "utf8":
		if IsMysqlBinaryType(column.OriginalType) {
			switch v := value.(type) {
			case string:
				result, _ := base64.StdEncoding.DecodeString(v)
				return result
			case []byte:
				return string(v)
			}
		}
		if strings.HasPrefix(column.OriginalType, "mysql:bit") {
			switch t := value.(type) {
			case json.Number:
				return t.String()
			}
		}
		if column.OriginalType == "pg:bytea" || strings.HasPrefix(column.OriginalType, "mysql:binary") || strings.HasPrefix(column.OriginalType, "mysql:blob") || column.OriginalType == "ydb:String" {
			switch t := value.(type) {
			case string:
				result, _ := base64.StdEncoding.DecodeString(t)
				return result
			case []byte:
				return t
			default:
				panic(fmt.Sprintf("impossible, originalType:%s, type(value):%T, value:%v", column.OriginalType, value, value))
			}
		}
		switch v := value.(type) {
		case *string:
			return *v
		case string:
			return v
		case []uint8:
			return string(v)
		}
		d, _ := json.Marshal(value)
		return string(d)
	case "any":
		if strings.HasPrefix(column.OriginalType, "mongo:bson") {
			val, err := json.Marshal(value)
			if err == nil {
				var repackedValue interface{}
				if jsonx.NewDefaultDecoder(bytes.NewReader(val)).Decode(&repackedValue) == nil {
					return repackedValue
				}
			}
		}
		rawVal, isString := value.(string)
		if !isString {
			return value
		}

		if column.OriginalType == "mysql:json" ||
			strings.HasPrefix(column.OriginalType, "ch:Decimal(") ||
			strings.HasPrefix(column.OriginalType, "ch:Nullable(Decimal(") {

			return rawVal
		}

		if strings.HasPrefix(column.OriginalType, "pg:timestamp") {
			// PostgreSQL `TIMESTAMP ...` is emitted as time.Time, not as string. And in PostgreSQL it has a form
			// different form the JSON serialization of this type. This special deserialization is necessary for
			// arrays - they have original type set to `any` and are not parsed as a normal timestamp.
			result, err := time.Parse(time.RFC3339Nano, rawVal)
			if err != nil {
				return rawVal
			}
			return result
		}
		if strings.HasPrefix(column.OriginalType, "pg:") {
			return rawVal
		}
		return tryUnmarshalJSON(rawVal)
	}

	return value
}

// maybeStringToNumeric unwrap string or stinger values into numeric
// actually WA for a bug - https://github.com/spf13/cast/issues/143
func maybeStringToNumeric(value interface{}) interface{} {
	if strval, ok := value.(string); ok {
		if strings.Contains(strval, ".") {
			f, _ := strconv.ParseFloat(strval, 64)
			return f
		}
		if strings.HasPrefix(strval, "-") {
			f, _ := strconv.ParseInt(strval, 10, 64)
			return f
		}
		f, _ := strconv.ParseUint(strval, 10, 64)
		return f
	}
	if v, ok := value.(time.Duration); ok {
		return int64(v)
	}
	if jsonNum, ok := value.(fmt.Stringer); ok {
		return maybeStringToNumeric(jsonNum.String())
	}
	return value
}

func TrimMySQLType(rawColumnType string) string {
	for idx, currRune := range rawColumnType {
		if (currRune < 'a' || currRune > 'z') && currRune != ':' {
			return rawColumnType[0:idx]
		}
	}
	return rawColumnType
}

func isMysqlBinaryTypeTrimmed(originalType string) bool {
	switch originalType {
	case
		"mysql:tinyblob", "mysql:blob", "mysql:mediumblob", "mysql:longblob",
		"mysql:binary", "mysql:varbinary", "mysql:bit",
		"mysql:geometry", "mysql:geomcollection", "mysql:geometrycollection",
		"mysql:point", "mysql:multipoint", "mysql:linestring", "mysql:multilinestring", "mysql:polygon", "mysql:multipolygon":
		return true
	}
	return false
}

func IsMysqlBinaryType(in string) bool {
	q := TrimMySQLType(in)
	return isMysqlBinaryTypeTrimmed(q)
}

func tryUnmarshalJSON(rawVal string) interface{} {
	var jsonb interface{}
	decoder := jsonx.NewDefaultDecoder(bytes.NewReader([]byte(rawVal)))
	if err := decoder.Decode(&jsonb); err != nil {
		return rawVal
	}
	if decoder.More() {
		return rawVal
	}
	return jsonb
}

func RestoreChangeItems(batch []ChangeItem) {
	schemas := map[TableID]map[string]ColSchema{}
	for _, item := range batch {
		if _, ok := schemas[item.TableID()]; ok {
			continue
		}
		schemas[item.TableID()] = map[string]ColSchema{}
		for _, col := range item.TableSchema.Columns() {
			schemas[item.TableID()][col.ColumnName] = col
		}
	}
	for _, changeItem := range batch {
		for i, name := range changeItem.ColumnNames {
			changeItem.ColumnValues[i] = Restore(schemas[changeItem.TableID()][name], changeItem.ColumnValues[i])
		}
		if changeItem.Kind == InsertKind {
			continue
		}
		for i, name := range changeItem.OldKeys.KeyNames {
			changeItem.OldKeys.KeyValues[i] = Restore(schemas[changeItem.TableID()][name], changeItem.OldKeys.KeyValues[i])
		}
	}
}

func UnmarshalChangeItems(changeItemsBuf []byte) ([]ChangeItem, error) {
	var batch []ChangeItem
	if err := jsonx.NewDefaultDecoder(bytes.NewReader(changeItemsBuf)).Decode(&batch); err != nil {
		return nil, xerrors.Errorf("unable to unmarshal changeItems - err: %w", err)
	}
	if err := ValidateChangeItems(batch); err != nil {
		return nil, xerrors.Errorf("changeItems are invalid - err: %w", err)
	}
	RestoreChangeItems(batch)

	// handle pg:enum_all_values
	for i := range batch {
		cols := batch[i].TableSchema.Columns()
		for j := range cols {
			if cols[j].Properties != nil && cols[j].Properties["pg:enum_all_values"] != nil {
				val := cols[j].Properties["pg:enum_all_values"]
				if valUnp, ok := val.([]interface{}); ok {
					newVal := make([]string, 0)
					for _, v := range valUnp {
						newVal = append(newVal, v.(string))
					}
					cols[j].Properties["pg:enum_all_values"] = newVal
				}
			}
		}
		batch[i].TableSchema = NewTableSchema(cols)
	}

	return batch, nil
}

func UnmarshalChangeItem(changeItemBuf []byte) (*ChangeItem, error) {
	var changeItem *ChangeItem
	if err := jsonx.NewDefaultDecoder(bytes.NewReader(changeItemBuf)).Decode(&changeItem); err != nil {
		return nil, xerrors.Errorf("unable to unmarshal changeItem - err: %w", err)
	}
	if err := ValidateChangeItem(changeItem); err != nil {
		return nil, xerrors.Errorf("changeItems are invalid - err: %w", err)
	}
	RestoreChangeItems([]ChangeItem{*changeItem})
	return changeItem, nil
}
