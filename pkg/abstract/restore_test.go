package abstract

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestRestore(t *testing.T) {
	assert.Equal(t, Restore(colSchema("", false), nil), nil)
	assert.Equal(t, Restore(colSchema("", false), uint64(1609448400)), uint64(1609448400))

	t.Run("dates", func(t *testing.T) {
		ts := time.Unix(1609459200, 0)
		ytdate, err := schema.NewDate(ts)
		require.NoError(t, err)
		ytts, err := schema.NewTimestamp(ts)
		require.NoError(t, err)
		ytdt, err := schema.NewDatetime(ts)
		require.NoError(t, err)
		// backward compatible yt-date types, remove in march
		assert.Equal(t, Restore(colSchema("date", false), int64(ytdate)), ts.UTC())
		assert.Equal(t, Restore(colSchema("datetime", false), int64(ytdt)), ts.UTC())
		assert.Equal(t, Restore(colSchema("timestamp", false), int64(ytts)), ts.UTC())

		assert.Equal(t, Restore(colSchema("Date", false), ts), ts)
		assert.Equal(t, Restore(colSchema("date", false), ts), ts)
		assert.Equal(t, Restore(colSchema("Date", false), &ts), ts)

		assert.Equal(t, Restore(colSchema("DateTime", false), ts), ts)
		assert.Equal(t, Restore(colSchema("datetime", false), ts), ts)
		assert.Equal(t, Restore(colSchema("int64", false), ts), int64(-1609459200000000000))
		assert.Equal(t, Restore(colSchema("uint64", false), ts), int64(1609459200000000000))
		assert.Equal(t, Restore(colSchema("int32", false), ts), int64(-1609459200))
		assert.Equal(t, Restore(colSchema("uint32", false), ts), int64(1609459200))
	})

	//assert.Equal(t, Restore(colSchema("utf8", false), ts), "2021-01-01T03:00:00+03:00") // format RFC3339Nano use local tz, maybe we can use time.LoadLocation
	//assert.Equal(t, Restore(colSchema("string", false), ts), "2021-01-01T03:00:00+03:00")
	//assert.Equal(t, Restore(colSchema("any", false), ts), "2021-01-01T03:00:00+03:00")

	uuid_, _ := uuid.FromString("cda6498a-235d-4f7e-ae19-661d41bc154c")
	assert.Equal(t, Restore(colSchema("", false), uuid_), "cda6498a-235d-4f7e-ae19-661d41bc154c")

	t.Run("numbers", func(t *testing.T) {
		assert.Equal(t, Restore(colSchema("int64", false), float64(-9223372036854775808)), int64(-9223372036854775808))
		assert.Equal(t, Restore(colSchema("int32", false), float64(-2147483648)), int32(-2147483648))
		assert.Equal(t, Restore(colSchema("int16", false), float64(-32768)), int16(-32768))
		assert.Equal(t, Restore(colSchema("int8", false), float64(-128)), int8(-128))

		assert.Equal(t, Restore(colSchema("uint64", false), json.Number("18446744073709551615")), uint64(18446744073709551615))
		assert.Equal(t, Restore(colSchema("uint64", false), json.Number("1609459200000000000")), uint64(1609459200000000000))
		assert.Equal(t, Restore(colSchema("uint32", false), float64(4294967295)), uint32(4294967295))
		assert.Equal(t, Restore(colSchema("uint16", false), float64(65535)), uint16(65535))
		assert.Equal(t, Restore(colSchema("uint8", false), float64(255)), uint8(255))

		assert.Equal(t, Restore(colSchema("double", false), float64(255)), float64(255))
		assert.Equal(t, Restore(colSchema("double", false), float32(255)), float64(255))
		assert.Equal(t, Restore(colSchema("double", false), "4567.89"), float64(4567.89))
		assert.Equal(t, Restore(colSchema("double", false), "0.00000021d"), nil) // TODO BUG TM-2945
		assert.Equal(t, Restore(colSchema("double", false), int32(-1609448400)), nil)
		assert.Equal(t, Restore(colSchema("double", false), "-1.7976931348623157e+308"), float64(-1.7976931348623157e+308))
	})

	t.Run("string", func(t *testing.T) {
		assert.Equal(t, Restore(colSchema("string", false), "string"), "string")
		assert.Equal(t, Restore(colSchema("string", false), []byte("string")), "string")
	})
	t.Run("complex", func(t *testing.T) {
		type someStruct struct {
			A string
			B int
		}
		structValue := someStruct{A: "value", B: 123}
		serializedStruct := "{\"A\":\"value\",\"B\":123}"
		unmarshaledStruct := map[string]interface{}{"A": "value", "B": json.Number("123")} // After unmarshal type is json.Number
		assert.Equal(t, Restore(colSchema("string", false), structValue), serializedStruct)

		assert.Equal(t, Restore(colSchema("any", false), structValue), structValue)
		assert.Equal(t, Restore(colSchema("any", true), structValue), structValue)

		assert.Equal(t, Restore(colSchema("any", false), serializedStruct), unmarshaledStruct)
		assert.Equal(t, Restore(colSchema("any", true), serializedStruct), unmarshaledStruct)
	})

	t.Run("bytes", func(t *testing.T) {
		mirrorElement := colSchema("utf8", false)
		mirrorElement.OriginalType = OriginalTypeMirrorBinary
		assert.Equal(t, Restore(mirrorElement, "blablabla"), "blablabla")
	})
}

func TestRestoreFloatInt64(t *testing.T) {
	col := colSchema("int64", false)
	assert.Equal(t, int64(-9223372036854775808), Restore(col, float64(-9223372036854775808)))
	assert.Equal(t, int64(-9223372036854775808), Restore(col, int(-9223372036854775808)))
	assert.Equal(t, int64(-1), Restore(col, uint(18446744073709551615))) // TODO: overflow
	assert.Equal(t, int64(345), Restore(col, float32(345)))
	assert.Equal(t, int64(345), Restore(col, float32(345.123)))
	assert.Equal(t, int64(4567), Restore(col, json.Number("4567")))
	assert.Equal(t, int64(0), Restore(col, ""))
	assert.Equal(t, int64(4567), Restore(col, "4567.89"))
	assert.Equal(t, int64(0), Restore(col, "7.00000021d")) // TODO: trim float part
	assert.Equal(t, int64(0), Restore(col, "not a number"))
	assert.Equal(t, int64(4567), Restore(col, json.Number("4567.89")))
}

func TestRestoreJSONB(t *testing.T) {
	col := colSchema("any", false)
	assert.Equal(t, Restore(col, "2022"), json.Number("2022"))
	assert.Equal(t, Restore(col, "2022-03-01 12:32:16"), "2022-03-01 12:32:16")
	assert.Equal(t, Restore(col, `{"a": 123}`), map[string]interface{}{"a": json.Number("123")})
}

func colSchema(datatype string, primary bool) ColSchema {
	return ColSchema{
		TableSchema:  "public",
		TableName:    "test",
		Path:         "test_col",
		ColumnName:   "test_col",
		DataType:     datatype,
		PrimaryKey:   primary,
		FakeKey:      false,
		Required:     false,
		Expression:   "",
		OriginalType: "",
	}
}

func TestRestorePgEnum(t *testing.T) {
	const pgEnumAllValues = "pg:enum_all_values"

	col := ColSchema{
		TableSchema:  "MyTableSchema",
		TableName:    "MyTableName",
		Path:         "",
		ColumnName:   "MyColumnName",
		DataType:     "any",
		PrimaryKey:   false,
		FakeKey:      false,
		Required:     false,
		Expression:   "",
		OriginalType: "pg:val",
		Properties:   nil,
	}
	col.AddProperty(pgEnumAllValues, []string{"a", "b"})

	currChangeItem := ChangeItem{
		TableSchema: NewTableSchema([]ColSchema{col}),
	}
	changeItems, err := UnmarshalChangeItems([]byte(`[` + currChangeItem.ToJSONString() + `]`))
	require.NoError(t, err)
	require.Len(t, changeItems, 1)
	outChangeItem := changeItems[0]
	val, ok := outChangeItem.TableSchema.Columns()[0].Properties[pgEnumAllValues]
	require.True(t, ok)
	assert.Equal(t, []string{"a", "b"}, val)
}
