package util

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/transferria/transferria/library/go/core/xerrors"
)

func MakeUnitedStructByKeyVal(copyVals bool, key interface{}, value interface{}) interface{} {
	return MakeUnitedStruct(copyVals, key, value)
}

func MakeUnitedStruct(copyVals bool, structs ...interface{}) interface{} {
	// make result type
	var sfs []reflect.StructField
	var vals []reflect.Value
	for _, currStruct := range structs {
		t := reflect.TypeOf(currStruct)
		v := reflect.ValueOf(currStruct)
		for i := 0; i < t.NumField(); i++ {
			currField := t.Field(i)
			sf := reflect.StructField{
				Name: currField.Name,
				Type: currField.Type,
				Tag:  currField.Tag,
			}
			sfs = append(sfs, sf)
			vals = append(vals, v.Field(i))
		}
	}
	st := reflect.StructOf(sfs)
	so := reflect.New(st)
	result := so.Interface()

	// fill result struct by values
	if copyVals {
		resultType := reflect.TypeOf(result).Elem()
		resultValue := reflect.ValueOf(result).Elem()
		for i := 0; i < resultType.NumField(); i++ {
			currFieldName := resultType.Field(i).Name
			resultValue.FieldByName(currFieldName).Set(vals[i])
		}
	}

	return result
}

func ExtractStructFromScanResult(scanResult interface{}, structForExtracting interface{}) interface{} {
	scanResultMap := scanResult.(map[string]interface{})

	unitedFieldNameToValue := make(map[string]reflect.Value)
	for k, v := range scanResultMap {
		v := reflect.ValueOf(v)
		unitedFieldNameToValue[k] = v
	}

	so := reflect.New(reflect.TypeOf(structForExtracting))
	result := so.Interface()

	resultType := reflect.TypeOf(result).Elem()
	resultValue := reflect.ValueOf(result).Elem()
	for i := 0; i < resultType.NumField(); i++ {
		resultTypeField := resultType.Field(i)
		currFieldName := resultTypeField.Name
		currTag := resultTypeField.Tag
		currDstType := resultTypeField.Type
		currFieldYsonName, ok := currTag.Lookup("yson")
		if !ok {
			continue
		}
		val, ok := unitedFieldNameToValue[currFieldYsonName]
		if !ok {
			panic(fmt.Sprintf("Field %s not found in unitedStruct", currFieldYsonName))
		}
		resultValue.FieldByName(currFieldName).Set(val.Convert(currDstType))
	}

	return result
}

func IsTwoStructTypesTheSame(a, b interface{}) bool {
	return reflect.TypeOf(a) == reflect.TypeOf(b)
}

func FieldsNumSerializedInYson(currStruct interface{}) int {
	result := 0
	resultType := reflect.TypeOf(currStruct)
	for i := 0; i < resultType.NumField(); i++ {
		_, ok := resultType.Field(i).Tag.Lookup("yson")
		if ok {
			result++
		}
	}
	return result
}

func IsFieldYsonKey(field reflect.StructField) bool {
	currTag := field.Tag
	currFieldYsonName, ok := currTag.Lookup("yson")
	if !ok {
		return false
	}

	parts := strings.Split(currFieldYsonName, ",")

	for _, part := range parts[1:] {
		switch part {
		case "key":
			return true
		}
	}
	return false
}

func ValidateKey(in interface{}) error {
	resultType := reflect.TypeOf(in)

	if resultType.NumField() != FieldsNumSerializedInYson(in) {
		return xerrors.Errorf("some of key columns not serialized to yson")
	}

	for i := 0; i < resultType.NumField(); i++ {
		if !IsFieldYsonKey(resultType.Field(i)) {
			return xerrors.Errorf("some of key columns not marked as key: %s", resultType.Field(i).Name)
		}
	}

	return nil
}

func ValidateVal(in interface{}) error {
	resultType := reflect.TypeOf(in)

	if resultType.NumField() != FieldsNumSerializedInYson(in) {
		return xerrors.Errorf("some of key columns not serialized to yson")
	}

	for i := 0; i < resultType.NumField(); i++ {
		if IsFieldYsonKey(resultType.Field(i)) {
			return xerrors.Errorf("some of val columns marked as key: %s", resultType.Field(i).Name)
		}
	}

	return nil
}
