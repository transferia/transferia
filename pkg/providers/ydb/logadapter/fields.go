//go:build !disable_ydb_provider

package logadapter

import (
	ydbLog "github.com/ydb-platform/ydb-go-sdk/v3/log"
	"go.ytsaurus.tech/library/go/core/log"
)

func fieldToField(field ydbLog.Field) log.Field {
	switch field.Type() {
	case ydbLog.IntType:
		return log.Int(field.Key(), field.IntValue())
	case ydbLog.Int64Type:
		return log.Int64(field.Key(), field.Int64Value())
	case ydbLog.StringType:
		return log.String(field.Key(), field.StringValue())
	case ydbLog.BoolType:
		return log.Bool(field.Key(), field.BoolValue())
	case ydbLog.DurationType:
		return log.Duration(field.Key(), field.DurationValue())
	case ydbLog.StringsType:
		return log.Strings(field.Key(), field.StringsValue())
	case ydbLog.ErrorType:
		return log.Error(field.ErrorValue())
	case ydbLog.StringerType:
		return log.String(field.Key(), field.Stringer().String())
	default:
		return log.Any(field.Key(), field.AnyValue())
	}
}

func ToCoreFields(fields []ydbLog.Field) []log.Field {
	ff := make([]log.Field, len(fields))
	for i, f := range fields {
		ff[i] = fieldToField(f)
	}
	return ff
}
