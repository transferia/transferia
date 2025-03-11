package debezium

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	debeziumcommon "github.com/transferia/transferia/pkg/debezium/common"
	"github.com/transferia/transferia/pkg/debezium/mysql"
	"github.com/transferia/transferia/pkg/debezium/pg"
	"github.com/transferia/transferia/pkg/debezium/ydb"
)

var prefixToNotDefaultReceiver map[string]debeziumcommon.NotDefaultReceiverDescription

func init() {
	// init this map into init() to avoid 'initialization loop'
	prefixToNotDefaultReceiver = map[string]debeziumcommon.NotDefaultReceiverDescription{
		"pg:":    pg.KafkaTypeToOriginalTypeToFieldReceiverFunc,
		"ydb:":   ydb.KafkaTypeToOriginalTypeToFieldReceiverFunc,
		"mysql:": mysql.KafkaTypeToOriginalTypeToFieldReceiverFunc,
	}
}

func handleFieldReceiverMatchers(fieldReceiver debeziumcommon.FieldReceiver, originalType *debeziumcommon.OriginalTypeInfo, schema *debeziumcommon.Schema) debeziumcommon.FieldReceiver {
	switch t := fieldReceiver.(type) {
	case *debeziumcommon.FieldReceiverMatchers:
		checkFieldReceiverArr := t.Matchers
		for _, matcher := range checkFieldReceiverArr {
			if matcher.IsMatched(originalType, schema) {
				return matcher
			}
		}
		return nil
	default:
		return fieldReceiver
	}
}

func findFieldReceiver(in debeziumcommon.NotDefaultReceiverDescription, originalType *debeziumcommon.OriginalTypeInfo, schema *debeziumcommon.Schema) debeziumcommon.FieldReceiver {
	if in == nil {
		return nil
	}
	originalTypeToFieldReceiverFunc, ok := in[debeziumcommon.KafkaType(schema.Type)]
	if !ok {
		return nil
	}
	if _, ok := originalTypeToFieldReceiverFunc[originalType.OriginalType]; ok { // match by full OriginalType
		return originalTypeToFieldReceiverFunc[originalType.OriginalType]
	} else {
		bracketIndex := strings.Index(originalType.OriginalType, "(")
		if bracketIndex != -1 {
			originalTypeUnparametrized := originalType.OriginalType[0:bracketIndex] // match by unparametrized OriginalType
			if _, ok := originalTypeToFieldReceiverFunc[originalTypeUnparametrized]; ok {
				return originalTypeToFieldReceiverFunc[originalTypeUnparametrized]
			}
		}

		if fieldReceiver, ok := originalTypeToFieldReceiverFunc[debeziumcommon.DTMatchByFunc]; ok { // match by matcher
			return handleFieldReceiverMatchers(fieldReceiver, originalType, schema)
		}
		return nil
	}
}

func getDatabaseSpecificReceiver(originalType string) (debeziumcommon.NotDefaultReceiverDescription, error) {
	if originalType == "" {
		return nil, nil
	}

	prefixIndex := strings.Index(originalType, ":")
	if prefixIndex == -1 {
		return nil, xerrors.Errorf("unable to extract prefix, str: %s", originalType)
	}
	prefix := originalType[0 : prefixIndex+1]

	return prefixToNotDefaultReceiver[prefix], nil
}

func receiveFieldChecked(ytType string, inSchemaDescr *debeziumcommon.Schema, val interface{}, originalType *debeziumcommon.OriginalTypeInfo) (interface{}, bool, error) {
	val, isAbsentVal, err := receiveField(inSchemaDescr, val, originalType, false)
	if err != nil {
		return nil, false, xerrors.Errorf("unable to receive value, field: %s, err: %w", inSchemaDescr.Field, err)
	}
	if isAbsentVal {
		return nil, true, nil
	}
	inType := debeziumcommon.KafkaType(inSchemaDescr.Type)
	resultTypes, ok := debeziumcommon.KafkaTypeToResultYTTypes[inType]
	if !ok {
		return nil, false, xerrors.Errorf("unknown kafka_type, field: %s", inSchemaDescr.Type)
	}
	assertValidateRetType(ytType, resultTypes.ResultType, resultTypes.AlternativeResultType, inSchemaDescr.Type)
	return val, false, nil
}

func receiveFieldColSchema(inSchemaDescr *debeziumcommon.Schema, originalType *debeziumcommon.OriginalTypeInfo) (*abstract.ColSchema, error) {
	colSchema := &abstract.ColSchema{
		TableSchema:  "",
		TableName:    "",
		Path:         "", // don't know what is it
		ColumnName:   inSchemaDescr.Field,
		DataType:     "", // will be filled in this function further
		PrimaryKey:   !inSchemaDescr.Optional,
		FakeKey:      false,                     // don't know what is it
		Required:     false,                     // don't know where is true - bcs even when PrimaryKey:true, Required:false
		Expression:   "",                        // for sure is empty - bcs we don't have such information
		OriginalType: originalType.OriginalType, // for sure is empty - bcs we don't have OriginalType info
		Properties:   nil,
	}
	if debeziumcommon.KafkaType(inSchemaDescr.Type) == debeziumcommon.KafkaTypeArray {
		colSchema.DataType = "any"
	} else {
		receiverDescription, err := getDatabaseSpecificReceiver(originalType.OriginalType)
		if err != nil {
			return nil, xerrors.Errorf("unable to get database-specific receiver")
		}
		fieldReceiverFunc := findFieldReceiver(receiverDescription, originalType, inSchemaDescr)
		if fieldReceiverFunc == nil {
			fieldReceiverFunc = debeziumcommon.TypeToDefault[debeziumcommon.KafkaType(inSchemaDescr.Type)]
			if fieldReceiverFunc != nil {
				fieldReceiverFunc = handleFieldReceiverMatchers(fieldReceiverFunc, originalType, inSchemaDescr)
			}
		}
		if fieldReceiverFunc == nil {
			return nil, xerrors.Errorf("unable to find field receiver - even default, for kafka type: %s", inSchemaDescr.Type)
		}

		if additionalInfo, ok := fieldReceiverFunc.(debeziumcommon.ContainsColSchemaAdditionalInfo); ok {
			additionalInfo.AddInfo(inSchemaDescr, colSchema)
		}
		colSchema.DataType = fieldReceiverFunc.YTType()
	}
	return colSchema, nil
}

func receiveField(inSchemaDescr *debeziumcommon.Schema, val interface{}, originalType *debeziumcommon.OriginalTypeInfo, intoArr bool) (interface{}, bool, error) {
	if val == nil {
		return nil, false, nil
	}
	if valStr, ok := val.(string); ok {
		if valStr == "__debezium_unavailable_value" {
			return nil, true, nil
		}
	}

	if debeziumcommon.KafkaType(inSchemaDescr.Type) == debeziumcommon.KafkaTypeArray {
		outVal, err := arrayReceive(inSchemaDescr, val, originalType, intoArr)
		if err != nil {
			return nil, false, xerrors.Errorf("unable to receive data from array")
		}
		return outVal, false, nil
	}

	// unpack values

	receiverDescription, err := getDatabaseSpecificReceiver(originalType.OriginalType)
	if err != nil {
		return "", false, xerrors.Errorf("unable to get database-specific receiver")
	}
	fieldReceiverFunc := findFieldReceiver(receiverDescription, originalType, inSchemaDescr)
	if fieldReceiverFunc == nil {
		fieldReceiverFunc = debeziumcommon.TypeToDefault[debeziumcommon.KafkaType(inSchemaDescr.Type)]
		if fieldReceiverFunc != nil {
			fieldReceiverFunc = handleFieldReceiverMatchers(fieldReceiverFunc, originalType, inSchemaDescr)
		}
	}
	if fieldReceiverFunc == nil {
		return nil, false, xerrors.Errorf("unable to find field receiver - even default, for kafka type: %s", inSchemaDescr.Type)
	}

	var valInt64 *int64
	var valBoolean *bool
	var valString *string
	var valFloat64 *float64
	err = extractVal(fieldReceiverFunc, val, &valInt64, &valBoolean, &valString, &valFloat64)
	if err != nil {
		return nil, false, xerrors.Errorf("unable to extract value, err: %w", err)
	}

	// assert

	if valInt64 == nil && valBoolean == nil && valString == nil && valFloat64 == nil {
		switch fieldReceiverFunc.(type) {
		case debeziumcommon.StructToFloat64, debeziumcommon.StructToString, debeziumcommon.AnyToDouble, debeziumcommon.AnyToAny:
		default:
			inSchemaDescrStr, _ := json.Marshal(inSchemaDescr)
			valStr, _ := json.Marshal(val)
			originalTypeStr, _ := json.Marshal(originalType)
			return nil, false, xerrors.Errorf("assert no one value extracted, inSchemaDescr:%s, val:%s, originalType:%s", inSchemaDescrStr, valStr, originalTypeStr)
		}
	}

	// convert values

	result, err := convertVal(fieldReceiverFunc, inSchemaDescr, val, originalType, intoArr, valInt64, valBoolean, valString, valFloat64)
	return result, false, err
}

func extractVal(
	receiver debeziumcommon.FieldReceiver,
	val interface{},
	valInt64 **int64,
	valBoolean **bool,
	valString **string,
	valFloat64 **float64,
) error {
	switch receiver.(type) {
	case debeziumcommon.Int8ToInt8,
		debeziumcommon.Int8ToUint8,
		debeziumcommon.Int16ToInt16,
		debeziumcommon.IntToInt32,
		debeziumcommon.IntToUint16,
		debeziumcommon.IntToUint32,
		debeziumcommon.IntToString,
		debeziumcommon.Int64ToInt64,
		debeziumcommon.Int64ToTime,
		debeziumcommon.DurationToInt64,
		debeziumcommon.Int64ToUint64: // uint64 is stored in int64
		switch v := val.(type) {
		case json.Number:
			valInt64Tmp, err := v.Int64()
			if err != nil {
				return xerrors.Errorf("unable to convert json.Number into Int64(), val: %s, err: %w", v.String(), err)
			}
			*valInt64 = &valInt64Tmp
		}
	case debeziumcommon.BooleanToBoolean,
		debeziumcommon.BooleanToInt8,
		debeziumcommon.BooleanToBytes,
		debeziumcommon.BooleanToString:
		switch v := val.(type) {
		case bool:
			*valBoolean = &v
		}
	case debeziumcommon.StringToString,
		debeziumcommon.StringToTime,
		debeziumcommon.StringToAny,
		debeziumcommon.StringToBytes:
		switch v := val.(type) {
		case string:
			*valString = &v
		case json.Number:
			valStr := v.String()
			*valString = &valStr
		}
	case debeziumcommon.Float64ToFloat32,
		debeziumcommon.Float64ToFloat64:
		switch v := val.(type) {
		case json.Number:
			valFloat64Tmp, err := v.Float64()
			if err != nil {
				return xerrors.Errorf("unable to convert json.Number into Float64(), val: %s, err: %w", v.String(), err)
			}
			*valFloat64 = &valFloat64Tmp
		}
	case debeziumcommon.StructToFloat64:
	case debeziumcommon.StructToString:
	case debeziumcommon.AnyToDouble:
	case debeziumcommon.AnyToAny:
	default:
		return xerrors.Errorf("unknown receiver type: %T", receiver)
	}
	return nil
}

func convertVal(
	receiver debeziumcommon.FieldReceiver,
	inSchemaDescr *debeziumcommon.Schema,
	val interface{},
	originalType *debeziumcommon.OriginalTypeInfo,
	intoArr bool,
	valInt64 *int64,
	valBoolean *bool,
	valString *string,
	valFloat64 *float64,
) (interface{}, error) {
	var result interface{}
	var err error

	switch fieldReceiverObj := receiver.(type) {
	case debeziumcommon.Int8ToInt8:
		result, err = fieldReceiverObj.Do(*valInt64, originalType, inSchemaDescr, intoArr)
	case debeziumcommon.Int8ToUint8:
		result, err = fieldReceiverObj.Do(*valInt64, originalType, inSchemaDescr, intoArr)
	case debeziumcommon.Int16ToInt16:
		result, err = fieldReceiverObj.Do(*valInt64, originalType, inSchemaDescr, intoArr)
	case debeziumcommon.IntToInt32:
		result, err = fieldReceiverObj.Do(*valInt64, originalType, inSchemaDescr, intoArr)
	case debeziumcommon.IntToUint32:
		result, err = fieldReceiverObj.Do(*valInt64, originalType, inSchemaDescr, intoArr)
	case debeziumcommon.IntToUint16:
		result, err = fieldReceiverObj.Do(*valInt64, originalType, inSchemaDescr, intoArr)
	case debeziumcommon.IntToString:
		result, err = fieldReceiverObj.Do(*valInt64, originalType, inSchemaDescr, intoArr)
	case debeziumcommon.Int64ToInt64:
		result, err = fieldReceiverObj.Do(*valInt64, originalType, inSchemaDescr, intoArr)
	case debeziumcommon.Int64ToUint64:
		result, err = fieldReceiverObj.Do(*valInt64, originalType, inSchemaDescr, intoArr)
	case debeziumcommon.DurationToInt64:
		result, err = fieldReceiverObj.Do(time.Duration(*valInt64), originalType, inSchemaDescr, intoArr)
	case debeziumcommon.Int64ToTime:
		result, err = fieldReceiverObj.Do(*valInt64, originalType, inSchemaDescr, intoArr)
	case debeziumcommon.BooleanToBoolean:
		result, err = fieldReceiverObj.Do(*valBoolean, originalType, inSchemaDescr, intoArr)
	case debeziumcommon.BooleanToInt8:
		result, err = fieldReceiverObj.Do(*valBoolean, originalType, inSchemaDescr, intoArr)
	case debeziumcommon.BooleanToBytes:
		result, err = fieldReceiverObj.Do(*valBoolean, originalType, inSchemaDescr, intoArr)
	case debeziumcommon.BooleanToString:
		result, err = fieldReceiverObj.Do(*valBoolean, originalType, inSchemaDescr, intoArr)
	case debeziumcommon.StringToString:
		result, err = fieldReceiverObj.Do(*valString, originalType, inSchemaDescr, intoArr)
	case debeziumcommon.StringToTime:
		result, err = fieldReceiverObj.Do(*valString, originalType, inSchemaDescr, intoArr)
	case debeziumcommon.StringToBytes:
		result, err = fieldReceiverObj.Do(*valString, originalType, inSchemaDescr, intoArr)
	case debeziumcommon.StringToAny:
		result, err = fieldReceiverObj.Do(*valString, originalType, inSchemaDescr, intoArr)
	case debeziumcommon.Float64ToFloat32:
		result, err = fieldReceiverObj.Do(*valFloat64, originalType, inSchemaDescr, intoArr)
	case debeziumcommon.Float64ToFloat64:
		result, err = fieldReceiverObj.Do(*valFloat64, originalType, inSchemaDescr, intoArr)
	case debeziumcommon.StructToFloat64:
		result, err = fieldReceiverObj.Do(val, originalType, inSchemaDescr, intoArr)
	case debeziumcommon.StructToString:
		result, err = fieldReceiverObj.Do(val, originalType, inSchemaDescr, intoArr)
	case debeziumcommon.AnyToDouble:
		result, err = fieldReceiverObj.Do(val, originalType, inSchemaDescr, intoArr)
	case debeziumcommon.AnyToAny:
		result, err = fieldReceiverObj.Do(val, originalType, inSchemaDescr, intoArr)
	default:
		return nil, xerrors.Errorf("unknown receiver type: %T", receiver)
	}
	if err != nil {
		return nil, xerrors.Errorf("unable to receive field, err: %w", err)
	}
	return result, nil
}

func arrayReceive(s *debeziumcommon.Schema, v interface{}, originalType *debeziumcommon.OriginalTypeInfo, _ bool) (interface{}, error) {
	if v == nil {
		return nil, nil
	}
	resultVal := make([]interface{}, 0)
	vArr := v.([]interface{})
	items := debeziumcommon.Schema(*s.Items)
	for _, el := range vArr {
		elVal, isAbsent, err := receiveField(&items, el, originalType.GetArrElemTypeDescr(), true)
		if err != nil {
			return nil, xerrors.Errorf("unable to receive array's element: %w", err)
		}
		if isAbsent {
			return nil, xerrors.Errorf("array can't contains absent values")
		}
		resultVal = append(resultVal, elVal)
	}
	return resultVal, nil
}

func assertValidateRetType(retType string, resultType string, alternativeResultType []string, inType string) {
	if retType == resultType {
		return
	}
	for _, el := range alternativeResultType {
		if retType == el {
			return
		}
	}
	panic(fmt.Sprintf("%s type converted to %s, what is undocumented", inType, retType))
}
