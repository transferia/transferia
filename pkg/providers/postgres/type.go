//go:build !disable_postgres_provider

package postgres

import (
	"reflect"
	"strings"
	"time"

	"github.com/jackc/pgtype"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/yt/go/schema"
)

// This is a copy of the pgx driver internal global map `nameValues`:
// https://github.com/transferia/transferia/arcadia/vendor/github.com/jackc/pgtype/pgtype.go?rev=10676203#L924-924
// We want to override the fallback type for unknown types, and use
// GenericBinary instead of GenericText when we have to.
// To do that, we use our own implementation of InitializeDataTypes function,
// which is, luckily, configurable:
// https://github.com/transferia/transferia/arcadia/vendor/github.com/jackc/pgtype/pgtype.go?rev=10676203#L370-380
// Unfortunately, the driver does not expose its internal map with the
// well-known PostgreSQL types which is used by the default implementation of
// InitializeDataTypes, so we have to copy that map into our code.
var nameValues map[string]pgtype.Value

func init() {
	nameValues = map[string]pgtype.Value{
		"_aclitem":       &pgtype.ACLItemArray{},
		"_bool":          &pgtype.BoolArray{},
		"_bpchar":        &pgtype.BPCharArray{},
		"_bytea":         &pgtype.ByteaArray{},
		"_cidr":          &pgtype.CIDRArray{},
		"_date":          &pgtype.DateArray{},
		"_float4":        &pgtype.Float4Array{},
		"_float8":        &pgtype.Float8Array{},
		"_inet":          &pgtype.InetArray{},
		"_int2":          &pgtype.Int2Array{},
		"_int4":          &pgtype.Int4Array{},
		"_int8":          &pgtype.Int8Array{},
		"_numeric":       &pgtype.NumericArray{},
		"_text":          &pgtype.TextArray{},
		"_timestamp":     &pgtype.TimestampArray{},
		"_timestamptz":   &pgtype.TimestamptzArray{},
		"_uuid":          &pgtype.UUIDArray{},
		"_varchar":       &pgtype.VarcharArray{},
		"_json":          &pgtype.JSONArray{},
		"_jsonb":         &pgtype.JSONBArray{},
		"aclitem":        &pgtype.ACLItem{},
		"bit":            &pgtype.Bit{},
		"bool":           &pgtype.Bool{},
		"box":            &pgtype.Box{},
		"bpchar":         &pgtype.BPChar{},
		"bytea":          &pgtype.Bytea{},
		"char":           &pgtype.QChar{},
		"cid":            &pgtype.CID{},
		"cidr":           &pgtype.CIDR{},
		"circle":         &pgtype.Circle{},
		"date":           &pgtype.Date{},
		"daterange":      &pgtype.Daterange{},
		"float4":         &pgtype.Float4{},
		"float8":         &pgtype.Float8{},
		"hstore":         &pgtype.Hstore{},
		"inet":           &pgtype.Inet{},
		"int2":           &pgtype.Int2{},
		"int4":           &pgtype.Int4{},
		"int4range":      &pgtype.Int4range{},
		"int4multirange": &pgtype.Int4multirange{},
		"int8":           &pgtype.Int8{},
		"int8range":      &pgtype.Int8range{},
		"int8multirange": &pgtype.Int8multirange{},
		"interval":       &pgtype.Interval{},
		"json":           &pgtype.JSON{},
		"jsonb":          &pgtype.JSONB{},
		"line":           &pgtype.Line{},
		"lseg":           &pgtype.Lseg{},
		"macaddr":        &pgtype.Macaddr{},
		"name":           &pgtype.Name{},
		"numeric":        &pgtype.Numeric{},
		"numrange":       &pgtype.Numrange{},
		"nummultirange":  &pgtype.Nummultirange{},
		"oid":            &pgtype.OIDValue{},
		"path":           &pgtype.Path{},
		"point":          &pgtype.Point{},
		"polygon":        &pgtype.Polygon{},
		"record":         &pgtype.Record{},
		"text":           &pgtype.Text{},
		"tid":            &pgtype.TID{},
		"timestamp":      &pgtype.Timestamp{},
		"timestamptz":    &pgtype.Timestamptz{},
		"tsrange":        &pgtype.Tsrange{},
		"_tsrange":       &pgtype.TsrangeArray{},
		"tstzrange":      &pgtype.Tstzrange{},
		"_tstzrange":     &pgtype.TstzrangeArray{},
		"unknown":        &pgtype.Unknown{},
		"uuid":           &pgtype.UUID{},
		"varbit":         &pgtype.Varbit{},
		"varchar":        &pgtype.Varchar{},
		"xid":            &pgtype.XID{},
	}
}

// This function is very much like pgtype.(*ConnInfo).InitializeDataTypes, but
// the catch-all type for the unknown OIDs is configurable. The pgtype's
// implementation hardcodes it to GenericText, while we want to get
// GenericBinary in some cases.
// defaultType must be a (possibly nil) pointer (this is required by pgx).
func initializeDataTypes(connInfo *pgtype.ConnInfo, nameOIDs map[string]uint32, defaultType pgtype.Value) {
	for name, oid := range nameOIDs {
		var value pgtype.Value
		if t, ok := nameValues[name]; ok {
			value = reflect.New(reflect.ValueOf(t).Elem().Type()).Interface().(pgtype.Value)
		} else {
			value = reflect.New(reflect.TypeOf(defaultType).Elem()).Interface().(pgtype.Value)
		}
		connInfo.RegisterDataType(pgtype.DataType{Value: value, Name: name, OID: oid})
	}
}

func ClearOriginalType(pgType string) string {
	if strings.HasSuffix(pgType, "[]") {
		return "[]"
	}
	pgType = strings.TrimPrefix(pgType, "pg:")
	switch {
	case IsPgTypeTimestampWithTimeZoneUnprefixed(pgType):
		return "TIMESTAMP WITH TIME ZONE"
	case IsPgTypeTimestampWithoutTimeZoneUnprefixed(pgType):
		return "TIMESTAMP WITHOUT TIME ZONE"
	case IsPgTypeTimeWithTimeZoneUnprefixed(pgType):
		return "TIME WITH TIME ZONE"
	case IsPgTypeTimeWithoutTimeZoneUnprefixed(pgType):
		return "TIME WITHOUT TIME ZONE"
	case strings.HasPrefix(pgType, "numeric"):
		return "NUMERIC"
	case strings.HasPrefix(pgType, "character("):
		return "CHARACTER(N)"
	case strings.HasPrefix(pgType, "character varying"):
		return "CHARACTER VARYING"
	case strings.HasPrefix(pgType, "bit("):
		return "BIT(N)"
	case strings.HasPrefix(pgType, "bit varying("):
		return "BIT VARYING(N)"
	}
	return strings.ToUpper(pgType)
}

var pgTypeTimestampWithoutTimeZone = map[string]bool{
	"timestamp without time zone":    true,
	"timestamp(0) without time zone": true,
	"timestamp(1) without time zone": true,
	"timestamp(2) without time zone": true,
	"timestamp(3) without time zone": true,
	"timestamp(4) without time zone": true,
	"timestamp(5) without time zone": true,
	"timestamp(6) without time zone": true,
}

func IsPgTypeTimestampWithoutTimeZone(originalType string) bool {
	if strings.HasPrefix(originalType, "pg:") {
		return IsPgTypeTimestampWithoutTimeZoneUnprefixed(strings.TrimPrefix(originalType, "pg:"))
	}
	return false
}

func IsPgTypeTimestampWithoutTimeZoneUnprefixed(in string) bool {
	return pgTypeTimestampWithoutTimeZone[in]
}

var pgTypeTimestampWithTimeZone = map[string]bool{
	"timestamp with time zone":    true,
	"timestamp(0) with time zone": true,
	"timestamp(1) with time zone": true,
	"timestamp(2) with time zone": true,
	"timestamp(3) with time zone": true,
	"timestamp(4) with time zone": true,
	"timestamp(5) with time zone": true,
	"timestamp(6) with time zone": true,
}

func IsPgTypeTimestampWithTimeZone(originalType string) bool {
	if strings.HasPrefix(originalType, "pg:") {
		return IsPgTypeTimestampWithTimeZoneUnprefixed(strings.TrimPrefix(originalType, "pg:"))
	}
	return false
}

func IsPgTypeTimestampWithTimeZoneUnprefixed(in string) bool {
	return pgTypeTimestampWithTimeZone[in]
}

var pgTypeTimeWithTimeZone = map[string]bool{
	"time with time zone":    true,
	"time(0) with time zone": true,
	"time(1) with time zone": true,
	"time(2) with time zone": true,
	"time(3) with time zone": true,
	"time(4) with time zone": true,
	"time(5) with time zone": true,
	"time(6) with time zone": true,
}

func IsPgTypeTimeWithTimeZoneUnprefixed(in string) bool {
	return pgTypeTimeWithTimeZone[in]
}

func IsPgTypeTimeWithTimeZone(originalType string) bool {
	if strings.HasPrefix(originalType, "pg:") {
		return IsPgTypeTimeWithTimeZoneUnprefixed(strings.TrimPrefix(originalType, "pg:"))
	}
	return false
}

var pgTypeTimeWithoutTimeZone = map[string]bool{
	"time without time zone":    true,
	"time(1) without time zone": true,
	"time(0) without time zone": true,
	"time(2) without time zone": true,
	"time(3) without time zone": true,
	"time(4) without time zone": true,
	"time(5) without time zone": true,
	"time(6) without time zone": true,
}

func IsPgTypeTimeWithoutTimeZoneUnprefixed(in string) bool {
	return pgTypeTimeWithoutTimeZone[in]
}

func IsPgTypeTimeWithoutTimeZone(originalType string) bool {
	if strings.HasPrefix(originalType, "pg:") {
		return IsPgTypeTimeWithoutTimeZoneUnprefixed(strings.TrimPrefix(originalType, "pg:"))
	}
	return false
}

func PgTypeToYTType(pgType string) schema.Type {
	pgType = strings.TrimPrefix(pgType, "pg:")
	if strings.HasSuffix(pgType, "[]") {
		return schema.TypeAny
	}
	if strings.HasPrefix(pgType, "character varying") {
		return schema.TypeString
	}
	if IsPgTypeTimestampWithoutTimeZoneUnprefixed(pgType) {
		return schema.TypeTimestamp
	}
	if IsPgTypeTimeWithTimeZoneUnprefixed(pgType) {
		return schema.TypeString
	}
	if IsPgTypeTimeWithoutTimeZoneUnprefixed(pgType) {
		return schema.TypeString
	}
	if IsPgTypeTimestampWithTimeZoneUnprefixed(pgType) {
		return schema.TypeTimestamp
	}
	if strings.HasPrefix(pgType, "numeric") {
		return schema.TypeFloat64
	}
	switch pgType {
	case "date":
		return schema.TypeDate
	case "data", "uuid", "name", "text", "interval", "char", "abstime", "money":
		return schema.TypeString
	case "boolean":
		return schema.TypeBoolean
	case "bigint":
		return schema.TypeInt64
	case "smallint":
		return schema.TypeInt16
	case "integer":
		return schema.TypeInt32
	case "real", "double precision":
		return schema.TypeFloat64
	case "bytea":
		return schema.TypeBytes
	default:
		return schema.TypeAny
	}
}

func IsUserDefinedType(col *abstract.ColSchema) bool {
	return strings.HasPrefix(col.OriginalType, "pg:USER-DEFINED")
}

func DaterangeToString(t []time.Time) string {
	return "[" + t[0].UTC().Format("2006-01-02") + "," + t[1].UTC().Format("2006-01-02") + ")"
}

func BuildColSchemaArrayElement(colSchema abstract.ColSchema) abstract.ColSchema {
	elemColSchema := colSchema
	elemColSchema.OriginalType = GetArrElemTypeDescr(elemColSchema.OriginalType)
	elemColSchema.DataType = string(PgTypeToYTType(elemColSchema.OriginalType))
	return elemColSchema
}

func GetArrElemTypeDescr(originalType string) string {
	return strings.TrimSuffix(originalType, "[]")
}

func UnwrapArrayElement(in string) string {
	if len(in) < 2 {
		return in
	}
	if in[0] == '\'' && in[len(in)-1] == '\'' {
		return in[1 : len(in)-1]
	} else {
		return in
	}
}
