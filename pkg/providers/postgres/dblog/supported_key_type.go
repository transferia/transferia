package dblog

import (
	"strings"

	"github.com/transferia/transferia/pkg/dblog"
	"github.com/transferia/transferia/pkg/util/set"
)

var supportedTypesArr = []string{
	"boolean",
	"bit",
	"varbit",

	"real",
	"smallint",
	"smallserial",
	"integer",
	"serial",
	"bigint",
	"bigserial",
	"oid",
	"tid",

	"double precision",

	"char",
	"varchar",
	"name",

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

	"_jsonb",
	"_numeric",
	"_text",
	"_timestamp",
	"_timestamptz",
	"_uuid",
	"_varchar",
	"_bool",
	"_bpchar",
	"_bytea",
	"_cidr",
	"_date",
	"_float4",
	"_float8",
	"_inet",
	"_int2",
	"_int4",
	"_int8",
	"bpchar",
	"float4",
	"float8",
	"int2",
	"int4",
	"int8",
	"bool",
	"nummultirange",
	"int4multirange",
	"int8multirange",
}

var unsupportedTypesArr = []string{
	"json",       // could not identify a comparison function
	"hstore",     // has no default operator class for access method "btree"
	"ltree",      // has no default operator class for access method "btree"
	"point",      // has no default operator class for access method "btree"
	"polygon",    // has no default operator class for access method "btree"
	"line",       // has no default operator class for access method "btree"
	"circle",     // has no default operator class for access method "btree"
	"box",        // has no default operator class for access method "btree"
	"path",       // has no default operator class for access method "btree"
	"aclitem",    // could not identify a comparison function
	"_aclitem",   // could not identify a comparison function
	"cid",        // has no default operator class for access method "btree"
	"record",     // pseudo-type
	"unknown",    // pseudo-type
	"xid",        // has no default operator class for access method "btree"
	"lseg",       // has no default operator class for access method "btree"
	"_json",      // could not identify a comparison function
	"_tsrange",   // failed to execute SELECT: ERROR: malformed range literal: "[2023-01-01 00:00:00" (SQLSTATE 22P02). Need to rewrite where statement
	"_tstzrange", // failed to execute SELECT: ERROR: malformed range literal: "[2023-01-01 00:00:00Z" (SQLSTATE 22P02). Need to rewrite where statement
}

var supportedTypesSet = set.New(supportedTypesArr...)
var unsupportedTypesSet = set.New(unsupportedTypesArr...)

func CheckTypeCompatibility(keyType string) dblog.TypeSupport {
	normalKeyType := strings.Split(keyType, "(")[0]
	normalKeyType = strings.TrimPrefix(normalKeyType, "pg:")

	switch {
	case supportedTypesSet.Contains(normalKeyType):
		return dblog.TypeSupported
	case unsupportedTypesSet.Contains(normalKeyType):
		return dblog.TypeUnsupported
	default:
		return dblog.TypeUnknown
	}
}
