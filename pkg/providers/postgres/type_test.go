//go:build !disable_postgres_provider

package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestCanonizedPgTypeToYTTypeString(t *testing.T) {
	t.Run("bigint", func(t *testing.T) { require.Equal(t, schema.TypeInt64, PgTypeToYTType("bigint")) })
	t.Run("bit(1)", func(t *testing.T) { require.Equal(t, schema.TypeAny, PgTypeToYTType("bit(1)")) })
	t.Run("bit(8)", func(t *testing.T) { require.Equal(t, schema.TypeAny, PgTypeToYTType("bit(8)")) })
	t.Run("bit varying(8)", func(t *testing.T) { require.Equal(t, schema.TypeAny, PgTypeToYTType("bit varying(8)")) })
	t.Run("boolean", func(t *testing.T) { require.Equal(t, schema.TypeBoolean, PgTypeToYTType("boolean")) })
	t.Run("bytea", func(t *testing.T) { require.Equal(t, schema.TypeBytes, PgTypeToYTType("bytea")) })
	t.Run("character(1)", func(t *testing.T) { require.Equal(t, schema.TypeAny, PgTypeToYTType("character(1)")) })
	t.Run("character(4)", func(t *testing.T) { require.Equal(t, schema.TypeAny, PgTypeToYTType("character(4)")) })
	t.Run("character varying(256)", func(t *testing.T) { require.Equal(t, schema.TypeString, PgTypeToYTType("character varying(256)")) })
	t.Run("character varying(5)", func(t *testing.T) { require.Equal(t, schema.TypeString, PgTypeToYTType("character varying(5)")) })
	t.Run("character varying", func(t *testing.T) { require.Equal(t, schema.TypeString, PgTypeToYTType("character varying")) })
	t.Run("cidr", func(t *testing.T) { require.Equal(t, schema.TypeAny, PgTypeToYTType("cidr")) })
	t.Run("daterange", func(t *testing.T) { require.Equal(t, schema.TypeAny, PgTypeToYTType("daterange")) })
	t.Run("date", func(t *testing.T) { require.Equal(t, schema.TypeDate, PgTypeToYTType("date")) })
	t.Run("double precision", func(t *testing.T) { require.Equal(t, schema.TypeFloat64, PgTypeToYTType("double precision")) })
	t.Run("inet", func(t *testing.T) { require.Equal(t, schema.TypeAny, PgTypeToYTType("inet")) })
	t.Run("int4range", func(t *testing.T) { require.Equal(t, schema.TypeAny, PgTypeToYTType("int4range")) })
	t.Run("int8range", func(t *testing.T) { require.Equal(t, schema.TypeAny, PgTypeToYTType("int8range")) })
	t.Run("integer", func(t *testing.T) { require.Equal(t, schema.TypeInt32, PgTypeToYTType("integer")) })
	t.Run("interval", func(t *testing.T) { require.Equal(t, schema.TypeString, PgTypeToYTType("interval")) })
	t.Run("json", func(t *testing.T) { require.Equal(t, schema.TypeAny, PgTypeToYTType("json")) })
	t.Run("jsonb", func(t *testing.T) { require.Equal(t, schema.TypeAny, PgTypeToYTType("jsonb")) })
	t.Run("macaddr", func(t *testing.T) { require.Equal(t, schema.TypeAny, PgTypeToYTType("macaddr")) })
	t.Run("money", func(t *testing.T) { require.Equal(t, schema.TypeString, PgTypeToYTType("money")) })
	t.Run("numeric", func(t *testing.T) { require.Equal(t, schema.TypeFloat64, PgTypeToYTType("numeric")) })
	t.Run("numrange", func(t *testing.T) { require.Equal(t, schema.TypeAny, PgTypeToYTType("numrange")) })
	t.Run("oid", func(t *testing.T) { require.Equal(t, schema.TypeAny, PgTypeToYTType("oid")) })
	t.Run("point", func(t *testing.T) { require.Equal(t, schema.TypeAny, PgTypeToYTType("point")) })
	t.Run("citext", func(t *testing.T) { require.Equal(t, schema.TypeAny, PgTypeToYTType("citext")) })
	t.Run("hstore", func(t *testing.T) { require.Equal(t, schema.TypeAny, PgTypeToYTType(`hstore`)) })
	t.Run("real", func(t *testing.T) { require.Equal(t, schema.TypeFloat64, PgTypeToYTType("real")) })
	t.Run("smallint", func(t *testing.T) { require.Equal(t, schema.TypeInt16, PgTypeToYTType("smallint")) })
	t.Run("text", func(t *testing.T) { require.Equal(t, schema.TypeString, PgTypeToYTType("text")) })
	t.Run("tsrange", func(t *testing.T) { require.Equal(t, schema.TypeAny, PgTypeToYTType("tsrange")) })
	t.Run("tstzrange", func(t *testing.T) { require.Equal(t, schema.TypeAny, PgTypeToYTType("tstzrange")) })
	t.Run("uuid", func(t *testing.T) { require.Equal(t, schema.TypeString, PgTypeToYTType("uuid")) })
	t.Run("xml", func(t *testing.T) { require.Equal(t, schema.TypeAny, PgTypeToYTType("xml")) })
	t.Run("character varying[]", func(t *testing.T) { require.Equal(t, schema.TypeAny, PgTypeToYTType("pg:character varying[]")) })
	t.Run("timestamp without time zone", func(t *testing.T) {
		require.Equal(t, schema.TypeTimestamp, PgTypeToYTType("timestamp without time zone"))
		require.Equal(t, schema.TypeTimestamp, PgTypeToYTType("timestamp(0) without time zone"))
		require.Equal(t, schema.TypeTimestamp, PgTypeToYTType("timestamp(1) without time zone"))
		require.Equal(t, schema.TypeTimestamp, PgTypeToYTType("timestamp(6) without time zone"))
	})
	t.Run("timestamp with time zone", func(t *testing.T) {
		require.Equal(t, schema.TypeTimestamp, PgTypeToYTType("timestamp with time zone"))
		require.Equal(t, schema.TypeTimestamp, PgTypeToYTType("timestamp(0) with time zone"))
		require.Equal(t, schema.TypeTimestamp, PgTypeToYTType("timestamp(1) with time zone"))
		require.Equal(t, schema.TypeTimestamp, PgTypeToYTType("timestamp(6) with time zone"))
	})
	t.Run("time with time zone", func(t *testing.T) {
		require.Equal(t, schema.TypeString, PgTypeToYTType("time with time zone"))
		require.Equal(t, schema.TypeString, PgTypeToYTType("time(0) with time zone"))
		require.Equal(t, schema.TypeString, PgTypeToYTType("time(1) with time zone"))
		require.Equal(t, schema.TypeString, PgTypeToYTType("time(6) with time zone"))
	})
	t.Run("time without time zone", func(t *testing.T) {
		require.Equal(t, schema.TypeString, PgTypeToYTType("time without time zone"))
		require.Equal(t, schema.TypeString, PgTypeToYTType("time(0) without time zone"))
		require.Equal(t, schema.TypeString, PgTypeToYTType("time(1) without time zone"))
		require.Equal(t, schema.TypeString, PgTypeToYTType("time(6) without time zone"))
	})
}
