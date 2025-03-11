package dblog

import (
	"strings"

	"github.com/transferria/transferria/pkg/util/set"
)

var supportedTypesArr = []string{
	"boolean",
	"bit",
	"varbit",

	"smallint",
	"smallserial",
	"integer",
	"serial",
	"bigint",
	"bigserial",
	"oid",

	"double precision",

	"char",
	"varchar",

	"character",
	"character varying",
	"timestamptz",
	"timestamp with time zone",
	"timestamp without time zone",
	"timetz",
	"time with time zone",
	"time without time zone",
	"interval",

	"bytea",

	"jsonb",

	"uuid",

	"inet",
	"int4range",
	"int8range",
	"numrange",
	"tsrange",
	"tstzrange",
	"daterange",

	"float",
	"int",
	"text",

	"date",
	"time",

	"numeric",
	"decimal",
	"money",

	"cidr",
	"macaddr",
	"citext",
}

var supportedTypes = set.New(supportedTypesArr...)

func IsSupportedKeyType(keyType string) bool {
	normalKeyType := strings.Split(keyType, "(")[0]
	return supportedTypes.Contains(strings.TrimPrefix(normalKeyType, "pg:"))
}
