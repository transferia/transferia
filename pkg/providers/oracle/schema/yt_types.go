package schema

// YTType* — значения abstract.ColSchema.DataType, совпадающие с константами
// go.ytsaurus.tech/yt/go/schema.Type (arcadia/yt/go/schema/schema.go).
// Дублируются без импорта yt/go/schema.
const (
	YTTypeInt8      = "int8"
	YTTypeInt16     = "int16"
	YTTypeInt32     = "int32"
	YTTypeInt64     = "int64"
	YTTypeFloat32   = "float"
	YTTypeFloat64   = "double"
	YTTypeString    = "utf8"
	YTTypeBytes     = "string"
	YTTypeDatetime  = "datetime"
	YTTypeTimestamp = "timestamp"
	YTTypeInterval  = "interval"
)
