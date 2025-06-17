//go:build !disable_postgres_provider

package postgres

import (
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"go.ytsaurus.tech/yt/go/schema"
)

func init() {
	typesystem.SourceRules(ProviderType, map[schema.Type][]string{
		schema.TypeInt64:   {"BIGINT"},
		schema.TypeInt32:   {"INTEGER"},
		schema.TypeInt16:   {"SMALLINT"},
		schema.TypeInt8:    {},
		schema.TypeUint64:  {},
		schema.TypeUint32:  {},
		schema.TypeUint16:  {},
		schema.TypeUint8:   {},
		schema.TypeFloat32: {},
		schema.TypeFloat64: {"NUMERIC", "REAL", "DOUBLE PRECISION"},
		schema.TypeBytes:   {"BIT(N)", "BIT VARYING(N)", "BYTEA", "BIT", "BIT VARYING"},
		schema.TypeString: {
			"CHARACTER VARYING", "DATA", "UUID", "NAME", "TEXT",
			"INTERVAL",
			"TIME WITH TIME ZONE",
			"TIME WITHOUT TIME ZONE",
			"CHAR", "ABSTIME", "MONEY",
		},
		schema.TypeBoolean:   {"BOOLEAN"},
		schema.TypeDate:      {"DATE"},
		schema.TypeDatetime:  {},
		schema.TypeTimestamp: {"TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP WITH TIME ZONE"},
		schema.TypeInterval:  {},
		schema.TypeAny: {
			"ARRAY", "CHARACTER(N)", "CITEXT", "HSTORE", "JSON", "JSONB", "DATERANGE", "INT4RANGE", "INT8RANGE", "NUMRANGE", "POINT",
			"TSRANGE", "TSTZRANGE", "XML", "INET", "CIDR", "MACADDR", "OID",
			typesystem.RestPlaceholder,
		},
	})
	typesystem.TargetRule(ProviderType, map[schema.Type]string{
		schema.TypeInt64:     "BIGINT",
		schema.TypeInt32:     "INTEGER",
		schema.TypeInt16:     "SMALLINT",
		schema.TypeInt8:      "SMALLINT",
		schema.TypeUint64:    "BIGINT",
		schema.TypeUint32:    "INTEGER",
		schema.TypeUint16:    "SMALLINT",
		schema.TypeUint8:     "SMALLINT",
		schema.TypeFloat32:   "REAL",
		schema.TypeFloat64:   "DOUBLE PRECISION",
		schema.TypeBytes:     "BYTEA",
		schema.TypeString:    "TEXT",
		schema.TypeBoolean:   "BOOLEAN",
		schema.TypeDate:      "DATE",
		schema.TypeDatetime:  "TIMESTAMP WITHOUT TIME ZONE",
		schema.TypeTimestamp: "TIMESTAMP WITHOUT TIME ZONE",
		schema.TypeAny:       "JSONB",
	})
}
