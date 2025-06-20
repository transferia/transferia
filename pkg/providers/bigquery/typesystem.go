//go:build !disable_bigquery_provider

package bigquery

import (
	"cloud.google.com/go/bigquery"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"go.ytsaurus.tech/yt/go/schema"
)

func init() {
	typesystem.TargetRule(ProviderType, map[schema.Type]string{
		schema.TypeInt64:     string(bigquery.BigNumericFieldType),
		schema.TypeInt32:     string(bigquery.IntegerFieldType),
		schema.TypeInt16:     string(bigquery.IntegerFieldType),
		schema.TypeInt8:      string(bigquery.IntegerFieldType),
		schema.TypeUint64:    string(bigquery.BigNumericFieldType),
		schema.TypeUint32:    string(bigquery.IntegerFieldType),
		schema.TypeUint16:    string(bigquery.IntegerFieldType),
		schema.TypeUint8:     string(bigquery.IntegerFieldType),
		schema.TypeFloat32:   string(bigquery.FloatFieldType),
		schema.TypeFloat64:   string(bigquery.FloatFieldType),
		schema.TypeBytes:     string(bigquery.BytesFieldType),
		schema.TypeString:    string(bigquery.StringTargetType),
		schema.TypeBoolean:   string(bigquery.BooleanFieldType),
		schema.TypeAny:       string(bigquery.JSONFieldType),
		schema.TypeDate:      string(bigquery.DateFieldType),
		schema.TypeDatetime:  string(bigquery.DateTimeFieldType),
		schema.TypeTimestamp: string(bigquery.TimestampFieldType),
	})
}
