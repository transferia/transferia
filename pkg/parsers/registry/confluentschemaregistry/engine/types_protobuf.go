package engine

import (
	"bytes"
	"encoding/json"

	"github.com/jhump/protoreflect/dynamic"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util/jsonx"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"google.golang.org/protobuf/types/descriptorpb"
)

var protoSchemaTypes = map[string]ytschema.Type{
	descriptorpb.FieldDescriptorProto_TYPE_DOUBLE.String():   ytschema.TypeFloat64,
	descriptorpb.FieldDescriptorProto_TYPE_FLOAT.String():    ytschema.TypeFloat32,
	descriptorpb.FieldDescriptorProto_TYPE_INT64.String():    ytschema.TypeInt64,
	descriptorpb.FieldDescriptorProto_TYPE_UINT64.String():   ytschema.TypeUint64,
	descriptorpb.FieldDescriptorProto_TYPE_INT32.String():    ytschema.TypeInt32,
	descriptorpb.FieldDescriptorProto_TYPE_FIXED64.String():  ytschema.TypeUint64,
	descriptorpb.FieldDescriptorProto_TYPE_FIXED32.String():  ytschema.TypeUint32,
	descriptorpb.FieldDescriptorProto_TYPE_BOOL.String():     ytschema.TypeBoolean,
	descriptorpb.FieldDescriptorProto_TYPE_STRING.String():   ytschema.TypeString,
	descriptorpb.FieldDescriptorProto_TYPE_GROUP.String():    "", // unsupported
	descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.String():  ytschema.TypeAny,
	descriptorpb.FieldDescriptorProto_TYPE_BYTES.String():    ytschema.TypeBytes,
	descriptorpb.FieldDescriptorProto_TYPE_UINT32.String():   ytschema.TypeUint32,
	descriptorpb.FieldDescriptorProto_TYPE_ENUM.String():     ytschema.TypeString,
	descriptorpb.FieldDescriptorProto_TYPE_SFIXED32.String(): ytschema.TypeInt32,
	descriptorpb.FieldDescriptorProto_TYPE_SFIXED64.String(): ytschema.TypeInt64,
	descriptorpb.FieldDescriptorProto_TYPE_SINT32.String():   ytschema.TypeInt32,
	descriptorpb.FieldDescriptorProto_TYPE_SINT64.String():   ytschema.TypeInt64,
}

func unpackVal(inputVal interface{}, fieldType string, repeated bool) (interface{}, error) {
	if repeated {
		val, err := unpackRepeatedVal(inputVal, fieldType)
		if err != nil {
			return nil, xerrors.Errorf("unable to unpack repeated value, err: %w", err)
		}
		return val, nil
	}
	return unpackNotRepeatedVal(inputVal, fieldType)
}

func unpackRepeatedVal(inputVal interface{}, fieldType string) (interface{}, error) {
	var outVals interface{}
	switch valsUnp := inputVal.(type) {
	case []interface{}:
		outSliceVals := make([]interface{}, 0, len(valsUnp))
		for _, valUnp := range valsUnp {
			unpackedVal, err := unpackNotRepeatedVal(valUnp, fieldType)
			if err != nil {
				return nil, xerrors.Errorf("unable to unpack one of repeated values, err: %w", err)
			}
			outSliceVals = append(outSliceVals, unpackedVal)
		}
		outVals = outSliceVals
	case map[interface{}]interface{}:
		outMapVals := make(map[string]interface{})
		for k, v := range valsUnp {
			unpackedVal, err := unpackNotRepeatedVal(v, fieldType)
			if err != nil {
				return nil, xerrors.Errorf("unable to unpack map value, err: %w", err)
			}
			switch kUnp := k.(type) {
			case string:
				outMapVals[kUnp] = unpackedVal
			default:
				return nil, xerrors.Errorf("these types are not supported yet as a map key, key:%v, type:%T", kUnp, kUnp)
			}
		}
		outVals = outMapVals
	default:
		return nil, xerrors.Errorf("these types are not supported yet, type:%T", valsUnp)
	}
	return outVals, nil
}

func unpackNotRepeatedVal(val interface{}, fieldType string) (interface{}, error) {
	switch fieldType {
	case descriptorpb.FieldDescriptorProto_TYPE_DOUBLE.String():
		return val.(float64), nil
	case descriptorpb.FieldDescriptorProto_TYPE_FLOAT.String():
		return val.(float32), nil
	case descriptorpb.FieldDescriptorProto_TYPE_INT64.String():
		return val.(int64), nil
	case descriptorpb.FieldDescriptorProto_TYPE_UINT64.String():
		return val.(uint64), nil
	case descriptorpb.FieldDescriptorProto_TYPE_INT32.String():
		return val.(int32), nil
	case descriptorpb.FieldDescriptorProto_TYPE_FIXED64.String():
		return val.(uint64), nil
	case descriptorpb.FieldDescriptorProto_TYPE_FIXED32.String():
		return val.(uint32), nil
	case descriptorpb.FieldDescriptorProto_TYPE_BOOL.String():
		return val.(bool), nil
	case descriptorpb.FieldDescriptorProto_TYPE_STRING.String():
		return val.(string), nil
	case descriptorpb.FieldDescriptorProto_TYPE_GROUP.String():
		return nil, xerrors.New("type GROUP is unsupported")
	case descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.String():
		switch t := val.(type) {
		case *dynamic.Message:
			if t == nil {
				return nil, nil
			}
			result := make(map[string]interface{})
			for _, el := range t.GetKnownFields() {
				fieldName := el.GetName()
				fieldType := el.GetType().String()
				isRepeated := el.IsRepeated()
				val, err := unpackVal(t.GetFieldByName(fieldName), fieldType, isRepeated)
				if err != nil {
					return nil, xerrors.Errorf("unable to unpack nested field %s, err: %w", fieldName, err)
				}
				result[fieldName] = val
			}
			return result, nil
		default:
			vvv, err := json.Marshal(val)
			if err != nil {
				return nil, xerrors.Errorf("unable to marshal, err: %w", err)
			}
			var valMap interface{}
			err = jsonx.NewDefaultDecoder(bytes.NewBuffer(vvv)).Decode(&valMap)
			if err != nil {
				return nil, xerrors.Errorf("unable to decode value, err: %w", err)
			}
			return valMap, nil
		}
	case descriptorpb.FieldDescriptorProto_TYPE_BYTES.String():
		return val.([]byte), nil
	case descriptorpb.FieldDescriptorProto_TYPE_UINT32.String():
		return val.(uint32), nil
	case descriptorpb.FieldDescriptorProto_TYPE_ENUM.String():
		return val.(int32), nil
	case descriptorpb.FieldDescriptorProto_TYPE_SFIXED32.String():
		return val.(int32), nil
	case descriptorpb.FieldDescriptorProto_TYPE_SFIXED64.String():
		return val.(int64), nil
	case descriptorpb.FieldDescriptorProto_TYPE_SINT32.String():
		return val.(int32), nil
	case descriptorpb.FieldDescriptorProto_TYPE_SINT64.String():
		return val.(int64), nil
	default:
		return nil, xerrors.Errorf("unknown type: %s", fieldType)
	}
}
