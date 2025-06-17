//go:build !disable_elastic_provider

package elastic

import (
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"go.ytsaurus.tech/yt/go/schema"
)

func init() {
	typesystem.SourceRules(ProviderType, map[schema.Type][]string{
		schema.TypeInt64:   {"long"},
		schema.TypeInt32:   {"integer"},
		schema.TypeInt16:   {"short"},
		schema.TypeInt8:    {"byte"},
		schema.TypeUint64:  {"unsigned_long"},
		schema.TypeUint32:  {},
		schema.TypeUint16:  {},
		schema.TypeUint8:   {},
		schema.TypeFloat32: {"float", "half_float"},
		schema.TypeFloat64: {"double", "scaled_float", "rank_feature"},
		schema.TypeBytes:   {"binary"},
		schema.TypeString:  {"text", "ip", "constant_keyword", "match_only_text", "search_as_you_type"},
		schema.TypeBoolean: {"boolean"},
		schema.TypeAny: {
			"object", "nested", "join", "flattened", "integer_range", "float_range", "long_range", "double_range",
			"date_range", "ip_range", "keyword", "wildcard", "version", "aggregate_metric_double", "histogram",
			"completion", "dense_vector", "geo_point", "point", "rank_features", "geo_shape", "shape", "percolator",
		},
		schema.TypeDate:      {},
		schema.TypeDatetime:  {},
		schema.TypeTimestamp: {"date", "date_nanos"},
	})

	typesystem.TargetRule(ProviderType, map[schema.Type]string{
		schema.TypeInt64:     "long",
		schema.TypeInt32:     "integer",
		schema.TypeInt16:     "short",
		schema.TypeInt8:      "byte",
		schema.TypeUint64:    "unsigned_long",
		schema.TypeUint32:    "unsigned_long",
		schema.TypeUint16:    "unsigned_long",
		schema.TypeUint8:     "unsigned_long",
		schema.TypeFloat32:   "float",
		schema.TypeFloat64:   "double",
		schema.TypeBytes:     "binary",
		schema.TypeString:    "text",
		schema.TypeBoolean:   "boolean",
		schema.TypeAny:       "object",
		schema.TypeDate:      "date",
		schema.TypeDatetime:  "date",
		schema.TypeTimestamp: "date",
	})
}
