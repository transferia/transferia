package postgres

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/yt/go/schema"
)

type arrUint8 []uint8

func (a arrUint8) Value() (driver.Value, error) {
	return []uint8{'{', '}'}, nil
}

type arrUint8WithQuote []uint8

func (a arrUint8WithQuote) Value() (driver.Value, error) {
	return []uint8{'{', '"', 'q', '"', ':', '"', '\'', '"', '}'}, nil
}

func TestRepresent_AdditionalCoverage(t *testing.T) {
	baseTime := time.Date(2024, time.March, 3, 12, 30, 45, 123456000, time.UTC)

	type testCase struct {
		name     string
		inValue  interface{}
		inSchema abstract.ColSchema
		outValue string
	}

	testCases := []testCase{
		{
			name:     "nil value",
			inValue:  nil,
			inSchema: abstract.ColSchema{},
			outValue: "null",
		},
		{
			name:     "string escaped",
			inValue:  "it's",
			inSchema: abstract.ColSchema{DataType: schema.TypeString.String(), OriginalType: "pg:text"},
			outValue: "'it''s'",
		},
		{
			name:     "string escaped as bytes",
			inValue:  `path\to\user's\dir`,
			inSchema: abstract.ColSchema{DataType: schema.TypeBytes.String(), OriginalType: "pg:bytea"},
			outValue: `'path\\to\\user''s\\dir'`,
		},
		{
			name:     "byte slice to hex",
			inValue:  []byte{0x01, 0x02, 0xab},
			inSchema: abstract.ColSchema{DataType: schema.TypeBytes.String(), OriginalType: "pg:bytea"},
			outValue: `'\x0102ab'`,
		},
		{
			name:     "int",
			inValue:  int(-7),
			inSchema: abstract.ColSchema{DataType: schema.TypeInt64.String(), OriginalType: "pg:bigint"},
			outValue: "'-7'",
		},
		{
			name:     "int32",
			inValue:  int32(12),
			inSchema: abstract.ColSchema{DataType: schema.TypeInt32.String(), OriginalType: "pg:integer"},
			outValue: "'12'",
		},
		{
			name:     "int64",
			inValue:  int64(42),
			inSchema: abstract.ColSchema{DataType: schema.TypeInt64.String(), OriginalType: "pg:bigint"},
			outValue: "'42'",
		},
		{
			name:     "uint32",
			inValue:  uint32(11),
			inSchema: abstract.ColSchema{DataType: "uint32"},
			outValue: "'11'",
		},
		{
			name:     "uint64",
			inValue:  uint64(13),
			inSchema: abstract.ColSchema{DataType: "uint64"},
			outValue: "'13'",
		},
		{
			name:     "float64 uint branch",
			inValue:  42.99,
			inSchema: abstract.ColSchema{DataType: "uint64"},
			outValue: "'42'",
		},
		{
			name:     "float64 int branch by data type",
			inValue:  42.99,
			inSchema: abstract.ColSchema{DataType: schema.TypeInt64.String(), OriginalType: "pg:numeric(18,6)"},
			outValue: "'42'",
		},
		{
			name:     "float64 int branch by original type",
			inValue:  15.88,
			inSchema: abstract.ColSchema{DataType: schema.TypeFloat64.String(), OriginalType: "pg:bigint"},
			outValue: "'15'",
		},
		{
			name:     "float64 format g",
			inValue:  1.25,
			inSchema: abstract.ColSchema{DataType: schema.TypeFloat64.String(), OriginalType: "pg:double precision"},
			outValue: "'1.25'",
		},
		{
			name:     "float64 format f",
			inValue:  3.5,
			inSchema: abstract.ColSchema{DataType: schema.TypeString.String(), OriginalType: "pg:numeric(18,6)"},
			outValue: "'3.500000'",
		},
		{
			name:     "bool default branch",
			inValue:  true,
			inSchema: abstract.ColSchema{DataType: schema.TypeBoolean.String(), OriginalType: "pg:boolean"},
			outValue: "'true'",
		},
		{
			name:     "time value date",
			inValue:  baseTime,
			inSchema: abstract.ColSchema{DataType: schema.TypeDate.String(), OriginalType: "pg:date"},
			outValue: "'2024-03-03'",
		},
		{
			name:     "time value datetime",
			inValue:  baseTime,
			inSchema: abstract.ColSchema{DataType: schema.TypeDatetime.String(), OriginalType: "pg:timestamp"},
			outValue: "'2024-03-03 12:30:45Z'",
		},
		{
			name:     "time pointer timestamp",
			inValue:  &baseTime,
			inSchema: abstract.ColSchema{DataType: schema.TypeTimestamp.String(), OriginalType: "pg:timestamp"},
			outValue: "'2024-03-03 12:30:45.123456Z'",
		},
		{
			name:     "jsonb map",
			inValue:  map[string]interface{}{"k": "v"},
			inSchema: abstract.ColSchema{DataType: schema.TypeAny.String(), OriginalType: "pg:jsonb"},
			outValue: `'{"k":"v"}'`,
		},
		{
			name:     "enum from bytes",
			inValue:  []byte("it's"),
			inSchema: abstract.ColSchema{DataType: schema.TypeAny.String(), OriginalType: "pg:my_enum", Properties: map[abstract.PropertyKey]interface{}{EnumAllValues: []string{"a", "b"}}},
			outValue: "'it''s'",
		},
		{
			name:     "hstore from map",
			inValue:  map[string]interface{}{"k": "v"},
			inSchema: abstract.ColSchema{DataType: schema.TypeAny.String(), OriginalType: "pg:hstore"},
			outValue: "'k=>v'",
		},
		{
			name:     "postgres array empty",
			inValue:  []interface{}{},
			inSchema: abstract.ColSchema{DataType: schema.TypeAny.String(), OriginalType: "pg:integer[]"},
			outValue: "'{}'",
		},
		{
			name:     "postgres array with nil",
			inValue:  []interface{}{int64(1), nil, int64(3)},
			inSchema: abstract.ColSchema{DataType: schema.TypeAny.String(), OriginalType: "pg:integer[]"},
			outValue: "'{1,null,3}'",
		},
		{
			name:     "json array empty",
			inValue:  []interface{}{},
			inSchema: abstract.ColSchema{DataType: schema.TypeAny.String(), OriginalType: "pg:jsonb"},
			outValue: "'[]'",
		},
		{
			name:     "driver valuer json bytes",
			inValue:  arrUint8WithQuote{},
			inSchema: abstract.ColSchema{DataType: schema.TypeAny.String(), OriginalType: "pg:json"},
			outValue: `'{"q":"''"}'`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Represent(tc.inValue, tc.inSchema)
			require.NoError(t, err)
			require.Equal(t, tc.outValue, got)
		})
	}
}

func TestRepresent_CIDRCompatPath(t *testing.T) {
	cidr := &pgtype.CIDR{
		Status: pgtype.Present,
		IPNet: &net.IPNet{
			IP:   net.ParseIP("192.168.0.0"),
			Mask: net.CIDRMask(24, 32),
		},
	}

	got, err := Represent(cidr, abstract.ColSchema{DataType: schema.TypeString.String(), OriginalType: "pg:cidr"})
	require.NoError(t, err)
	require.Equal(t, "'192.168.0.0/24'", got)
}

func TestRepresentWithCast(t *testing.T) {
	t.Run("adds cast for standard pg type", func(t *testing.T) {
		got, err := RepresentWithCast(int64(10), abstract.ColSchema{
			DataType:     schema.TypeInt64.String(),
			OriginalType: "pg:bigint",
		})
		require.NoError(t, err)
		require.Equal(t, "'10'::bigint", got)
	})

	t.Run("no cast for user defined type", func(t *testing.T) {
		got, err := RepresentWithCast("admin", abstract.ColSchema{
			DataType:     schema.TypeAny.String(),
			OriginalType: "pg:USER-DEFINED",
		})
		require.NoError(t, err)
		require.Equal(t, "'admin'", got)
	})
}

func TestRepresentWithCastToWriter_EquivalentToRepresentWithCast(t *testing.T) {
	testCases := []struct {
		name      string
		value     any
		colSchema abstract.ColSchema
	}{
		{
			name:  "pg builtin type gets cast",
			value: int64(10),
			colSchema: abstract.ColSchema{
				DataType:     schema.TypeInt64.String(),
				OriginalType: "pg:bigint",
			},
		},
		{
			name:  "non pg type no cast",
			value: "abc",
			colSchema: abstract.ColSchema{
				DataType:     schema.TypeString.String(),
				OriginalType: "mysql:varchar",
			},
		},
		{
			name:  "user defined type no cast",
			value: "admin",
			colSchema: abstract.ColSchema{
				DataType:     schema.TypeAny.String(),
				OriginalType: "pg:USER-DEFINED",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expected, err := RepresentWithCast(tc.value, tc.colSchema)
			require.NoError(t, err)

			var sb bytes.Buffer
			err = representWithCastToWriter(&sb, tc.value, tc.colSchema)
			require.NoError(t, err)
			require.Equal(t, expected, sb.String())
		})
	}
}

func TestRepresent_NonPGSourceCompatibility(t *testing.T) {
	baseTime := time.Date(2024, time.April, 5, 6, 7, 8, 0, time.UTC)

	type testCase struct {
		name     string
		inValue  interface{}
		inSchema abstract.ColSchema
		outValue string
	}

	testCases := []testCase{
		{
			name:     "any without original type map is json",
			inValue:  map[string]interface{}{"k": "v"},
			inSchema: abstract.ColSchema{DataType: schema.TypeAny.String()},
			outValue: `'{"k":"v"}'`,
		},
		{
			name:     "array without original type uses json notation",
			inValue:  []interface{}{int64(1), "a"},
			inSchema: abstract.ColSchema{DataType: schema.TypeAny.String()},
			outValue: `'[1,"a"]'`,
		},
		{
			name:     "scalar string without original type",
			inValue:  "hello",
			inSchema: abstract.ColSchema{DataType: schema.TypeString.String()},
			outValue: "'hello'",
		},
		{
			name:     "scalar float without original type",
			inValue:  3.1415,
			inSchema: abstract.ColSchema{DataType: schema.TypeFloat64.String()},
			outValue: "'3.1415'",
		},
		{
			name:     "time without original type uses default timestamp format",
			inValue:  baseTime,
			inSchema: abstract.ColSchema{DataType: schema.TypeTimestamp.String()},
			outValue: "'2024-04-05 06:07:08Z'",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Represent(tc.inValue, tc.inSchema)
			require.NoError(t, err)
			require.Equal(t, tc.outValue, got)
		})
	}
}

func TestRepresentWithCast_NonPGSource(t *testing.T) {
	t.Run("no cast when original type is empty", func(t *testing.T) {
		got, err := RepresentWithCast(int64(17), abstract.ColSchema{
			DataType: schema.TypeInt64.String(),
		})
		require.NoError(t, err)
		require.Equal(t, "'17'", got)
	})

	t.Run("no cast when original type is not pg-prefixed", func(t *testing.T) {
		got, err := RepresentWithCast("abc", abstract.ColSchema{
			DataType:     schema.TypeString.String(),
			OriginalType: "mysql:varchar",
		})
		require.NoError(t, err)
		require.Equal(t, "'abc'", got)
	})
}

func TestRepresent(t *testing.T) {
	type testCase struct {
		inSchema string
		inValue  interface{}
		outValue string
	}

	testCases := []testCase{
		{ // json (got from yt-source yson) - output should be array in JSON notation
			`{"path":"","name":"t_yson","type":"any","key":false,"required":false,"original_type":"pg:jsonb"}`,
			[]interface{}{uint64(100), uint64(200), uint64(300)},
			`'[100,200,300]'`,
		},
		{ // array (got from pg-source int[]) - output should be in postgres-array notation
			`{"path":"","name":"arr_i","type":"any","key":false,"required":false,"original_type":"pg:integer[]"}`,
			[]interface{}{"1", "2"},
			`'{1,2}'`,
		},
		{ // array of strings
			`{"path":"","name":"arr_str","type":"utf8","key":false,"required":false,"original_type":"pg:character varying[]"}`,
			[]interface{}{"varchar_example", "varchar_example"},
			`'{varchar_example,varchar_example}'`,
		},
		{ // array of strings
			`{"path":"","name":"arr_character_varying_","type":"any","key":false,"required":false,"original_type":"pg:character varying(5)[]"}`,
			[]interface{}{"varc", "varc"},
			`'{varc,varc}'`,
		},
		{ // array of strings, any type but no orgiginal
			`{"path":"","name":"yt_arr","type":"any","key":false,"required":false}`,
			[]interface{}{"yandex_staff_history", "maps_yandex_staff_actual"},
			`'["yandex_staff_history","maps_yandex_staff_actual"]'`,
		},
		{ // YSON struct, any type but no orgiginal
			`{"path":"","name":"yt_arr","type":"any","key":false,"required":false}`,
			map[string]interface{}{"foo": 123, "bar": map[string]string{"baz": "booz"}},
			`'{"bar":{"baz":"booz"},"foo":123}'`,
		},
	}

	for i, currTestCase := range testCases {
		t.Run(fmt.Sprintf("tc/%v", i), func(t *testing.T) {
			var schema abstract.ColSchema
			err := json.Unmarshal([]byte(currTestCase.inSchema), &schema)
			require.NoError(t, err)
			newVal, err := Represent(currTestCase.inValue, schema)
			require.NoError(t, err)
			require.Equal(t, currTestCase.outValue, newVal)

			var sb bytes.Buffer
			err = recursiveRepresentToWriter(&sb, currTestCase.inValue, schema)
			require.NoError(t, err)
			require.Equal(t, newVal, sb.String())
		})
	}
}

func TestWriteQuotedInt64_MinMax(t *testing.T) {
	var b bytes.Buffer

	writeQuotedInt64(&b, math.MinInt64)
	require.Equal(t, "'-9223372036854775808'", b.String())

	b.Reset()
	writeQuotedInt64(&b, math.MaxInt64)
	require.Equal(t, "'9223372036854775807'", b.String())
}

func TestWriteQuotedUint64_MinMax(t *testing.T) {
	var b bytes.Buffer

	writeQuotedUint64(&b, 0)
	require.Equal(t, "'0'", b.String())

	b.Reset()
	writeQuotedUint64(&b, math.MaxUint64)
	require.Equal(t, "'18446744073709551615'", b.String())
}
