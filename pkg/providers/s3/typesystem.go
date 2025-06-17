//go:build !disable_s3_provider

package s3

import (
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"go.ytsaurus.tech/yt/go/schema"
)

func init() {
	typesystem.SourceRules(ProviderType, map[schema.Type][]string{
		schema.TypeInt64:     {"csv:int64", "parquet:INT64"},
		schema.TypeInt32:     {"csv:int32", "parquet:INT32"},
		schema.TypeInt16:     {"csv:int16", "parquet:INT16"},
		schema.TypeInt8:      {"csv:int8", "parquet:INT8"},
		schema.TypeUint64:    {"csv:uint64", "parquet:UINT64", "jsonl:uint64"},
		schema.TypeUint32:    {"csv:uint32", "parquet:UINT32"},
		schema.TypeUint16:    {"csv:uint16", "parquet:UINT16"},
		schema.TypeUint8:     {"csv:uint8", "parquet:UINT8"},
		schema.TypeFloat32:   {"csv:float", "parquet:FLOAT"},
		schema.TypeFloat64:   {"csv:double", "parquet:DOUBLE", "parquet:DECIMAL", "jsonl:number"},
		schema.TypeBytes:     {"csv:string", "parquet:BYTE_ARRAY", "parquet:FIXED_LEN_BYTE_ARRAY"},
		schema.TypeString:    {"csv:utf8", "parquet:STRING", "parquet:INT96", "jsonl:string", "jsonl:utf8"},
		schema.TypeBoolean:   {"csv:boolean", "parquet:BOOLEAN", "jsonl:boolean"},
		schema.TypeAny:       {"csv:any", typesystem.RestPlaceholder, "jsonl:object", "jsonl:array"},
		schema.TypeDate:      {"csv:date", "parquet:DATE"},
		schema.TypeDatetime:  {"csv:datetime"},
		schema.TypeTimestamp: {"csv:timestamp", "parquet:TIMESTAMP", "jsonl:timestamp"},
		schema.TypeInterval:  {"csv:interval"},
	})
	typesystem.TargetRule(ProviderType, map[schema.Type]string{
		schema.TypeInt64:     "",
		schema.TypeInt32:     "",
		schema.TypeInt16:     "",
		schema.TypeInt8:      "",
		schema.TypeUint64:    "",
		schema.TypeUint32:    "",
		schema.TypeUint16:    "",
		schema.TypeUint8:     "",
		schema.TypeFloat32:   "",
		schema.TypeFloat64:   "",
		schema.TypeBytes:     "",
		schema.TypeString:    "",
		schema.TypeBoolean:   "",
		schema.TypeAny:       "",
		schema.TypeDate:      "",
		schema.TypeDatetime:  "",
		schema.TypeTimestamp: "",
	})
}
