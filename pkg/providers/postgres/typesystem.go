package postgres

import (
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func init() {
	typesystem.SourceRules(ProviderType, map[ytschema.Type][]string{
		ytschema.TypeInt64:   {"BIGINT"},
		ytschema.TypeInt32:   {"INTEGER"},
		ytschema.TypeInt16:   {"SMALLINT"},
		ytschema.TypeInt8:    {},
		ytschema.TypeUint64:  {},
		ytschema.TypeUint32:  {},
		ytschema.TypeUint16:  {},
		ytschema.TypeUint8:   {},
		ytschema.TypeFloat32: {},
		ytschema.TypeFloat64: {"NUMERIC", "REAL", "DOUBLE PRECISION"},
		ytschema.TypeBytes:   {"BIT(N)", "BIT VARYING(N)", "BYTEA", "BIT", "BIT VARYING"},
		ytschema.TypeString: {
			"CHARACTER VARYING", "DATA", "UUID", "NAME", "TEXT",
			"INTERVAL",
			"TIME WITH TIME ZONE",
			"TIME WITHOUT TIME ZONE",
			"CHAR", "ABSTIME", "MONEY",
		},
		ytschema.TypeBoolean:   {"BOOLEAN"},
		ytschema.TypeDate:      {"DATE"},
		ytschema.TypeDatetime:  {},
		ytschema.TypeTimestamp: {timestampWithoutTimeZoneType, timestampWithTimeZoneType},
		ytschema.TypeInterval:  {},
		ytschema.TypeAny: {
			"ARRAY", "CHARACTER(N)", "CITEXT", "HSTORE", "JSON", "JSONB", "DATERANGE", "INT4RANGE", "INT8RANGE", "NUMRANGE", "POINT",
			"TSRANGE", "TSTZRANGE", "XML", "INET", "CIDR", "MACADDR", "OID",
			typesystem.RestPlaceholder,
		},
	})
	typesystem.TargetRule(ProviderType, map[ytschema.Type]string{
		ytschema.TypeInt64:     "BIGINT",
		ytschema.TypeInt32:     "INTEGER",
		ytschema.TypeInt16:     "SMALLINT",
		ytschema.TypeInt8:      "SMALLINT",
		ytschema.TypeUint64:    "BIGINT",
		ytschema.TypeUint32:    "INTEGER",
		ytschema.TypeUint16:    "SMALLINT",
		ytschema.TypeUint8:     "SMALLINT",
		ytschema.TypeFloat32:   "REAL",
		ytschema.TypeFloat64:   "DOUBLE PRECISION",
		ytschema.TypeBytes:     "BYTEA",
		ytschema.TypeString:    "TEXT",
		ytschema.TypeBoolean:   "BOOLEAN",
		ytschema.TypeDate:      "DATE",
		ytschema.TypeDatetime:  timestampWithoutTimeZoneType,
		ytschema.TypeTimestamp: timestampWithoutTimeZoneType,
		ytschema.TypeAny:       "JSONB",
	})
}
