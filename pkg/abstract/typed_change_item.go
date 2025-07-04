package abstract

import (
	"encoding/json"
	"reflect"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/util"
	"github.com/valyala/fastjson"
)

type TypedChangeItem ChangeItem
type TypedValue struct {
	Type  string `json:"type"`
	Value any    `json:"value"`
}

func (t *TypedChangeItem) MarshalJSON() ([]byte, error) {
	_, err := t.packTypedValues()
	if err != nil {
		return nil, xerrors.Errorf("unable to pack values: %w", err)
	}

	v := reflect.ValueOf(*t)

	jsonMap := map[string]TypedValue{}
	var errs util.Errors
	for i := 0; i < v.NumField(); i++ {
		jsonMap[reflect.TypeOf(*t).Field(i).Name], err = packTypedValue(v.Field(i).Interface())
		errs = util.AppendErr(errs, err)
	}
	if len(errs) > 0 {
		return nil, xerrors.Errorf("unable to pack change item: %w", errs)
	}

	return json.Marshal(jsonMap)
}

func (t *TypedChangeItem) UnmarshalJSON(data []byte) error {
	var errs util.Errors
	fastjson.MustParse(string(data)).GetObject().Visit(func(k []byte, v *fastjson.Value) {
		extractedV, parseErr := unpackTypedValue(v)
		if parseErr == nil {
			reflect.Indirect(reflect.ValueOf(t)).FieldByName(string(k)).Set(reflect.ValueOf(extractedV))
		}
		errs = util.AppendErr(errs, parseErr)
	})
	if len(errs) > 0 {
		return xerrors.Errorf("unable to unpack: %w", errs)
	}
	return nil
}

func (t *TypedChangeItem) packTypedValues() ([]TypedValue, error) {
	var res []TypedValue
	var errs util.Errors
	for _, val := range t.ColumnValues {
		typedV, err := packTypedValue(val)
		errs = util.AppendErr(errs, err)
		res = append(res, typedV)
	}
	if len(errs) > 0 {
		return nil, errs
	}
	return res, nil
}

func (t *TypedChangeItem) unpackTypedValues(v *fastjson.Value) ([]interface{}, error) {
	items, err := v.Array()
	if err != nil {
		return nil, xerrors.Errorf("unable to get array: %w", err)
	}
	var res []interface{}
	for _, item := range items {
		extractedV, err := unpackTypedValue(item)
		if err != nil {
			return nil, xerrors.Errorf("unable to unpack: %w", err)
		}
		res = append(res, extractedV)
	}
	return res, nil
}

func extractVal[T any](val []byte) (T, error) {
	var result T
	if err := json.Unmarshal(val, &result); err != nil {
		return result, xerrors.Errorf("unable to unmarshal: %T %w", result, err)
	}
	return result, nil
}

func unpackTypedValue(item *fastjson.Value) (any, error) {
	rawValue := []byte(item.Get("value").String())
	goTyp := string(item.Get("type").GetStringBytes())
	var val any
	var err error
	switch goTyp {
	case "nil":
		val = nil
	case typeName[int]():
		val, err = extractVal[int](rawValue)
	case typeName[bool]():
		val, err = extractVal[bool](rawValue)
	case typeName[int8]():
		val, err = extractVal[int8](rawValue)
	case typeName[int16]():
		val, err = extractVal[int16](rawValue)
	case typeName[int32]():
		val, err = extractVal[int32](rawValue)
	case typeName[int64]():
		val, err = extractVal[int64](rawValue)
	case typeName[uint8]():
		val, err = extractVal[uint8](rawValue)
	case typeName[uint16]():
		val, err = extractVal[uint16](rawValue)
	case typeName[uint32]():
		val, err = extractVal[uint32](rawValue)
	case typeName[uint64]():
		val, err = extractVal[uint64](rawValue)
	case typeName[float32]():
		val, err = extractVal[float32](rawValue)
	case typeName[float64]():
		val, err = extractVal[float64](rawValue)
	case typeName[json.Number]():
		val, err = extractVal[json.Number](rawValue)
	case typeName[string]():
		val, err = extractVal[string](rawValue)
	case typeName[[]byte](), typeName[[]uint8]():
		val, err = extractVal[[]byte](rawValue)
	case typeName[time.Duration]():
		val, err = extractVal[time.Duration](rawValue)
	case typeName[time.Time]():
		val, err = extractVal[time.Time](rawValue)
	case typeName[[]*string](): // Only Clickhouse source provide this type
		val, err = extractVal[[]*string](rawValue)
	case typeName[[]*uint8](): // Only Clickhouse source provide this type
		val, err = extractVal[[]*uint8](rawValue)
	case typeName[[]*uint16](): // Only Clickhouse source provide this type
		val, err = extractVal[[]*uint16](rawValue)
	case typeName[[]*uint32](): // Only Clickhouse source provide this type
		val, err = extractVal[[]*uint32](rawValue)
	case typeName[[]*uint64](): // Only Clickhouse source provide this type
		val, err = extractVal[[]*uint64](rawValue)
	case typeName[[]*int8](): // Only Clickhouse source provide this type
		val, err = extractVal[[]*int8](rawValue)
	case typeName[[]*int16](): // Only Clickhouse source provide this type
		val, err = extractVal[[]*int16](rawValue)
	case typeName[[]*int32](): // Only Clickhouse source provide this type
		val, err = extractVal[[]*int32](rawValue)
	case typeName[[]*int64](): // Only Clickhouse source provide this type
		val, err = extractVal[[]*int64](rawValue)
	case typeName[[]*float32](): // Only Clickhouse source provide this type
		val, err = extractVal[[]*float32](rawValue)
	case typeName[[]*float64](): // Only Clickhouse source provide this type
		val, err = extractVal[[]*float64](rawValue)
	case typeName[Kind](): // Only as internal change item fields
		val, err = extractVal[Kind](rawValue)
	case typeName[OldKeysType](): // Only as internal change item fields
		val, err = extractVal[OldKeysType](rawValue)
	case typeName[[]string](): // Only as internal change item fields
		val, err = extractVal[[]string](rawValue)
	case typeName[EventSize](): // Only as internal change item fields
		val, err = extractVal[EventSize](rawValue)
	case typeName[[]ColSchema](): // Only as internal change item fields
		if columns, extractErr := extractVal[[]ColSchema](rawValue); extractErr != nil {
			err = extractErr
		} else {
			val = NewTableSchema(columns)
		}
	case typeName[[]interface{}]():
		var res []interface{}
		if string(rawValue) == "null" {
			return res, nil
		}
		items, err := item.Get("value").Array()
		if err != nil {
			return nil, xerrors.Errorf("unable to array value: %w", err)
		}
		for _, subItem := range items {
			itemVal, err := unpackTypedValue(subItem)
			if err != nil {
				return nil, xerrors.Errorf("unable to unpack item: %w", err)
			}
			res = append(res, itemVal)
		}
		return res, nil
	case typeName[map[string]interface{}]():
		res := map[string]interface{}{}
		var errs util.Errors
		item.Get("value").GetObject().Visit(func(key []byte, v *fastjson.Value) {
			res[string(key)], err = unpackTypedValue(v)
			errs = util.AppendErr(errs, err)
		})
		if len(errs) > 0 {
			return nil, xerrors.Errorf("errors: %w", errs)
		}
		return res, nil
	case typeName[changeitem.QueueMessageMeta]():
		var s string
		err = json.Unmarshal(rawValue, &s)
		if err != nil {
			return nil, xerrors.Errorf("unable to unmarshal queue message meta: %w", err)
		}
		var result changeitem.QueueMessageMeta
		err = json.Unmarshal([]byte(s), &result)
		if err != nil {
			return nil, xerrors.Errorf("unable to unmarshal queue message meta: %w", err)
		}
		return result, nil
	default:
		return nil, xerrors.Errorf("unexpected type: %s", goTyp)
	}
	if err != nil {
		return nil, xerrors.Errorf("extract error: %w", err)
	}
	return val, nil
}

func packTypedValue(val any) (TypedValue, error) {
	if val == nil {
		return TypedValue{Value: nil, Type: "nil"}, nil
	}
	switch castedV := val.(type) {
	case []ColSchema: // repack into alt typ
		return TypedValue{Value: val, Type: typeNameFromValue(val)}, nil
	case int, int8, int16, int32, int64,
		uint8, uint16, uint32, uint64,
		float32, float64, json.Number,
		string,
		[]byte, []*string, []*uint8, []*uint16, []*uint32, []*uint64, //  clickhouse crap
		[]*float32, []*float64, []*int8, []*int16, []*int32, []*int64, //  clickhouse crap
		EventSize, []string, OldKeysType, Kind,
		time.Time, time.Duration,
		bool:
		return TypedValue{Value: val, Type: typeNameFromValue(val)}, nil
	case []interface{}:
		var items []TypedValue
		var errs util.Errors
		for _, item := range castedV {
			packedV, err := packTypedValue(item)
			errs = util.AppendErr(errs, err)
			items = append(items, packedV)
		}
		if len(errs) > 0 {
			return *new(TypedValue), xerrors.Errorf("many errs: %w", errs)
		}
		return TypedValue{Value: items, Type: typeNameFromValue(val)}, nil
	case map[string]interface{}:
		var errs util.Errors
		data := map[string]TypedValue{}
		for k, v := range castedV {
			var err error
			data[k], err = packTypedValue(v)
			errs = util.AppendErr(errs, err)
		}
		if len(errs) > 0 {
			return *new(TypedValue), xerrors.Errorf("many errs: %w", errs)
		}
		return TypedValue{Value: data, Type: typeNameFromValue(val)}, nil
	case [][]interface{}: // Clickhouse nested arrays
		var data [][]TypedValue
		var errs util.Errors
		for _, arr := range castedV {
			var items []TypedValue
			for _, item := range arr {
				packedV, err := packTypedValue(item)
				errs = util.AppendErr(errs, err)
				items = append(items, packedV)
			}
			data = append(data, items)
		}
		if len(errs) > 0 {
			return *new(TypedValue), xerrors.Errorf("many errs: %w", errs)
		}
		return TypedValue{Value: data, Type: typeNameFromValue(val)}, nil
	case *TableSchema:
		return TypedValue{Value: []ColSchema(castedV.Columns()), Type: typeNameFromValue([]ColSchema(castedV.Columns()))}, nil
	case changeitem.QueueMessageMeta:
		bytes, err := json.Marshal(castedV)
		if err != nil {
			return *new(TypedValue), xerrors.Errorf("unable to marshal: %w", err)
		}
		return TypedValue{Value: string(bytes), Type: typeNameFromValue(castedV)}, nil
	default:
		if reflect.TypeOf(val).Kind() == reflect.Pointer {
			var unpackedVal interface{}
			reflectV := reflect.ValueOf(val)
			if !reflectV.IsNil() && reflectV.IsValid() {
				unpackedVal = reflectV.Elem().Interface()
			}
			return packTypedValue(unpackedVal)
		} else {
			return *new(TypedValue), xerrors.Errorf("unexpected data type: %T", val)
		}
	}
}

func typeName[T any]() string {
	var value T
	return typeNameFromValue(value)
}

func typeNameFromValue(value any) string {
	rt := reflect.TypeOf(value)
	name := rt.String()
	return name
}
