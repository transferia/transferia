package logger

import (
	"reflect"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.uber.org/zap/zapcore"
)

func MarshalSanitizedObject(v interface{}, enc zapcore.ObjectEncoder) error {
	val := reflect.ValueOf(v)

	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil
		}
		val = val.Elem()
	}

	switch val.Kind() {
	case reflect.Struct:
		return marshalStructFields(val, enc)
	default:
		return xerrors.Errorf("cannot marshal object of kind %v, only struct type is supported", val.Kind())
	}
}

func marshalStructFields(val reflect.Value, enc zapcore.ObjectEncoder) error {
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)
		fieldValue := val.Field(i)

		if !fieldValue.CanInterface() {
			continue
		}

		fieldName := field.Name

		if field.Tag.Get("log") == "true" {
			if err := marshalField(fieldName, fieldValue, enc); err != nil {
				return xerrors.Errorf("cannot marshal field %v with error %w", fieldName, err)
			}
		} else {
			enc.AddString(fieldName, "***HIDDEN***")
		}
	}

	return nil
}

func marshalField(fieldName string, fieldValue reflect.Value, enc zapcore.ObjectEncoder) error {
	switch fieldValue.Kind() {
	case reflect.Struct:
		if err := enc.AddObject(fieldName, zapcore.ObjectMarshalerFunc(func(inner zapcore.ObjectEncoder) error {
			return marshalStructFields(fieldValue, inner)
		})); err != nil {
			return xerrors.Errorf("add struct error: %w", err)
		}
	case reflect.Map:
		if err := enc.AddObject(fieldName, zapcore.ObjectMarshalerFunc(func(inner zapcore.ObjectEncoder) error {
			iter := fieldValue.MapRange()
			for iter.Next() {
				innerKey := iter.Key()
				innerValue := iter.Value()
				if err := marshalField(innerKey.String(), innerValue, inner); err != nil {
					return xerrors.Errorf("add key %v: %w", innerKey, err)
				}
			}
			return nil
		})); err != nil {
			return xerrors.Errorf("cannot marshal map field %v: %w", fieldName, err)
		}
	case reflect.Ptr:
		if fieldValue.IsNil() {
			enc.AddString(fieldName, "nil")
			return nil
		}
		fieldValue = fieldValue.Elem()
		return marshalField(fieldName, fieldValue, enc)
	case reflect.Bool:
		enc.AddBool(fieldName, fieldValue.Bool())
	case reflect.Int:
		enc.AddInt(fieldName, int(fieldValue.Int()))
	case reflect.Int8:
		enc.AddInt8(fieldName, int8(fieldValue.Int()))
	case reflect.Int16:
		enc.AddInt16(fieldName, int16(fieldValue.Int()))
	case reflect.Int32:
		enc.AddInt32(fieldName, int32(fieldValue.Int()))
	case reflect.Int64:
		enc.AddInt64(fieldName, int64(fieldValue.Int()))
	case reflect.Uint:
		enc.AddUint(fieldName, uint(fieldValue.Uint()))
	case reflect.Uint8:
		enc.AddUint8(fieldName, uint8(fieldValue.Uint()))
	case reflect.Uint16:
		enc.AddUint16(fieldName, uint16(fieldValue.Uint()))
	case reflect.Uint32:
		enc.AddUint32(fieldName, uint32(fieldValue.Uint()))
	case reflect.Uint64:
		enc.AddUint64(fieldName, uint64(fieldValue.Uint()))
	case reflect.Uintptr:
		enc.AddUintptr(fieldName, uintptr(fieldValue.Uint()))
	case reflect.Float32:
		enc.AddFloat32(fieldName, float32(fieldValue.Float()))
	case reflect.Float64:
		enc.AddFloat64(fieldName, fieldValue.Float())
	case reflect.Complex64:
		enc.AddComplex64(fieldName, complex64(fieldValue.Complex()))
	case reflect.Complex128:
		enc.AddComplex128(fieldName, fieldValue.Complex())
	case reflect.Array, reflect.Slice:
		if err := enc.AddReflected(fieldName, fieldValue.Interface()); err != nil {
			return xerrors.Errorf("add array|slice failed %w", err)
		}
	case reflect.String:
		enc.AddString(fieldName, fieldValue.String())
	case reflect.Interface:
		if err := enc.AddReflected(fieldName, fieldValue.Interface()); err != nil {
			return xerrors.Errorf("add interface failed %w", err)
		}
	default:
		enc.AddString(fieldName, "***SKIPPED***")
	}
	return nil
}
