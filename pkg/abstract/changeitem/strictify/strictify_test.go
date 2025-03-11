package strictify

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract/changeitem"
	"github.com/transferria/transferria/pkg/abstract/typesystem/values"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
)

var integerTypesSch = changeitem.NewTableSchema([]changeitem.ColSchema{
	{
		ColumnName: "i8",
		DataType:   schema.TypeInt8.String(),
	},
	{
		ColumnName: "u8",
		DataType:   schema.TypeUint8.String(),
	},
	{
		ColumnName: "i16",
		DataType:   schema.TypeInt16.String(),
	},
	{
		ColumnName: "u16",
		DataType:   schema.TypeUint16.String(),
	},
	{
		ColumnName: "i32",
		DataType:   schema.TypeInt32.String(),
	},
	{
		ColumnName: "u32",
		DataType:   schema.TypeUint32.String(),
	},
	{
		ColumnName: "i64",
		DataType:   schema.TypeInt64.String(),
	},
	{
		ColumnName: "u64",
		DataType:   schema.TypeUint64.String(),
	},
})

func TestStrictifyIntegerTypesPositive(t *testing.T) {
	schFast := changeitem.MakeFastTableSchema(integerTypesSch.Columns())
	template := makeChangeItemTemplate(integerTypesSch)

	changeset := []changeitem.ChangeItem{
		changeItemWithValues(template, []interface{}{
			int8(0),   // i8
			uint8(0),  // u8
			int16(0),  // i16
			uint16(0), // u16
			int32(0),  // i32
			uint32(0), // u32
			int64(0),  // i64
			uint64(0), // u64
		}),
		changeItemWithValues(template, []interface{}{
			int8(-1),  // i8
			uint8(1),  // u8
			int16(-1), // i16
			uint16(1), // u16
			int32(-1), // i32
			uint32(1), // u32
			int64(-1), // i64
			uint64(1), // u64
		}),
		changeItemWithValues(template, []interface{}{
			int64(math.MinInt8),  // i8
			uint64(0),            // u8
			int64(math.MinInt16), // i16
			uint64(0),            // u16
			int64(math.MinInt32), // i32
			uint64(0),            // u32
			int64(math.MinInt64), // i64
			uint32(0),            // u64
		}),
		changeItemWithValues(template, []interface{}{
			int64(math.MaxInt8),    // i8
			uint64(math.MaxUint8),  // u8
			int64(math.MaxInt16),   // i16
			uint64(math.MaxUint16), // u16
			int(math.MaxInt32),     // i32 // This value is of special type
			uint64(math.MaxUint32), // u32
			int64(math.MaxInt64),   // i64
			uint64(math.MaxUint64), // u64
		}),
		changeItemWithValues(template, []interface{}{
			nil, // i8
			nil, // u8
			nil, // i16
			nil, // u16
			nil, // i32
			nil, // u32
			nil, // i64
			nil, // u64
		}),
	}

	for i := range changeset {
		logger.Log.Infof("strictifying an item [%d]", i)
		executePositiveStrictifyCheck(t, &changeset[i], schFast)
	}
}

func makeChangeItemTemplate(sch *changeitem.TableSchema) *changeitem.ChangeItem {
	schColNames := extractColumnNames(sch)
	tableSchema := "public"
	tableName := "t"
	return &changeitem.ChangeItem{
		Kind:        changeitem.InsertKind,
		Schema:      tableSchema,
		Table:       tableName,
		TableSchema: sch,
		ColumnNames: schColNames,
	}
}

func extractColumnNames(sch *changeitem.TableSchema) []string {
	result := make([]string, len(sch.Columns()))
	for i, cs := range sch.Columns() {
		result[i] = cs.ColumnName
	}
	return result
}

func changeItemWithValues(template *changeitem.ChangeItem, values []interface{}) changeitem.ChangeItem {
	result := *template
	result.ColumnValues = values
	if len(values) != len(result.ColumnNames) {
		panic(fmt.Sprintf("len(values) = %d, while len(ColumnNames) = %d", len(values), len(result.ColumnNames)))
	}
	return result
}

var strictTypeCheckerSet map[schema.Type]values.ValueTypeChecker = values.StrictValueTypeCheckers()

func executePositiveStrictifyCheck(t *testing.T, item *changeitem.ChangeItem, schFast changeitem.FastTableSchema) {
	require.NoError(t, Strictify(item, schFast))

	for i, cName := range item.ColumnNames {
		logger.Log.Infof("checking a column for strictness [%d]", i)
		sch, ok := schFast[changeitem.ColumnName(cName)]
		require.True(t, ok)
		checker, ok := strictTypeCheckerSet[schema.Type(sch.DataType)]
		require.True(t, ok)
		if item.ColumnValues[i] != nil {
			require.True(t, checker(item.ColumnValues[i]))
		}
	}
}

func TestStrictifyIntegerTypesNegativeWrongType(t *testing.T) {
	schFast := changeitem.MakeFastTableSchema(integerTypesSch.Columns())
	template := makeChangeItemTemplate(integerTypesSch)

	changeset := []changeitem.ChangeItem{
		changeItemWithValues(template, []interface{}{
			"wrong",   // i8
			uint8(0),  // u8
			int16(0),  // i16
			uint16(0), // u16
			int32(0),  // i32
			uint32(0), // u32
			int64(0),  // i64
			uint64(0), // u64
		}),
		changeItemWithValues(template, []interface{}{
			int8(0),   // i8
			"wrong",   // u8
			int16(0),  // i16
			uint16(0), // u16
			int32(0),  // i32
			uint32(0), // u32
			int64(0),  // i64
			uint64(0), // u64
		}),
		changeItemWithValues(template, []interface{}{
			int8(0),   // i8
			uint8(0),  // u8
			"wrong",   // i16
			uint16(0), // u16
			int32(0),  // i32
			uint32(0), // u32
			int64(0),  // i64
			uint64(0), // u64
		}),
		changeItemWithValues(template, []interface{}{
			int8(0),   // i8
			uint8(0),  // u8
			int16(0),  // i16
			"wrong",   // u16
			int32(0),  // i32
			uint32(0), // u32
			int64(0),  // i64
			uint64(0), // u64
		}),
		changeItemWithValues(template, []interface{}{
			int8(0),   // i8
			uint8(0),  // u8
			int16(0),  // i16
			uint16(0), // u16
			"wrong",   // i32
			uint32(0), // u32
			int64(0),  // i64
			uint64(0), // u64
		}),
		changeItemWithValues(template, []interface{}{
			int8(0),   // i8
			uint8(0),  // u8
			int16(0),  // i16
			uint16(0), // u16
			int32(0),  // i32
			"wrong",   // u32
			int64(0),  // i64
			uint64(0), // u64
		}),
		changeItemWithValues(template, []interface{}{
			int8(0),   // i8
			uint8(0),  // u8
			int16(0),  // i16
			uint16(0), // u16
			int32(0),  // i32
			uint32(0), // u32
			"wrong",   // i64
			uint64(0), // u64
		}),
		changeItemWithValues(template, []interface{}{
			int8(0),   // i8
			uint8(0),  // u8
			int16(0),  // i16
			uint16(0), // u16
			int32(0),  // i32
			uint32(0), // u32
			int64(0),  // i64
			"wrong",   // u64
		}),
	}

	for i := range changeset {
		logger.Log.Infof("strictifying an item [%d]", i)
		executeNegativeStrictifyCheck(t, &changeset[i], schFast)
	}
}

func executeNegativeStrictifyCheck(t *testing.T, item *changeitem.ChangeItem, schFast changeitem.FastTableSchema) {
	err := Strictify(item, schFast)
	require.Error(t, err)
	require.True(t, xerrors.Is(err, new(StrictifyError)))
	logger.Log.Info("negative strictify check successful", log.Error(err))
}

func TestStrictifyIntegerTypesNegativeGreaterThanUpperBound(t *testing.T) {
	schFast := changeitem.MakeFastTableSchema(integerTypesSch.Columns())
	template := makeChangeItemTemplate(integerTypesSch)

	changeset := []changeitem.ChangeItem{
		changeItemWithValues(template, []interface{}{
			int64(math.MaxInt8 + 1), // i8
			uint64(0),               // u8
			int64(0),                // i16
			uint64(0),               // u16
			int64(0),                // i32
			uint64(0),               // u32
			int64(0),                // i64
			uint64(0),               // u64
		}),
		changeItemWithValues(template, []interface{}{
			int64(0),                  // i8
			uint64(math.MaxUint8 + 1), // u8
			int64(0),                  // i16
			uint64(0),                 // u16
			int64(0),                  // i32
			uint64(0),                 // u32
			int64(0),                  // i64
			uint64(0),                 // u64
		}),
		changeItemWithValues(template, []interface{}{
			int64(0),                 // i8
			uint64(0),                // u8
			int64(math.MaxInt16 + 1), // i16
			uint64(0),                // u16
			int64(0),                 // i32
			uint64(0),                // u32
			int64(0),                 // i64
			uint64(0),                // u64
		}),
		changeItemWithValues(template, []interface{}{
			int64(0),                   // i8
			uint64(0),                  // u8
			int64(0),                   // i16
			uint64(math.MaxUint16 + 1), // u16
			int64(0),                   // i32
			uint64(0),                  // u32
			int64(0),                   // i64
			uint64(0),                  // u64
		}),
		changeItemWithValues(template, []interface{}{
			int64(0),                 // i8
			uint64(0),                // u8
			int64(0),                 // i16
			uint64(0),                // u16
			int64(math.MaxInt32 + 1), // i32
			uint64(0),                // u32
			int64(0),                 // i64
			uint64(0),                // u64
		}),
		changeItemWithValues(template, []interface{}{
			int64(0),                   // i8
			uint64(0),                  // u8
			int64(0),                   // i16
			uint64(0),                  // u16
			int64(0),                   // i32
			uint64(math.MaxUint32 + 1), // u32
			int64(0),                   // i64
			uint64(0),                  // u64
		}),
		changeItemWithValues(template, []interface{}{
			int64(0),              // i8
			uint64(0),             // u8
			int64(0),              // i16
			uint64(0),             // u16
			int64(0),              // i32
			uint64(0),             // u32
			"9223372036854775808", // i64 // workaround for unrepresentability of such a huge number
			uint64(0),             // u64
		}),
		changeItemWithValues(template, []interface{}{
			int64(0),               // i8
			uint64(0),              // u8
			int64(0),               // i16
			uint64(0),              // u16
			int64(0),               // i32
			uint64(0),              // u32
			int64(0),               // i64
			"18446744073709551616", // u64 // workaround for unrepresentability of such a huge number
		}),
	}

	for i := range changeset {
		logger.Log.Infof("strictifying an item [%d]", i)
		executeNegativeStrictifyCheck(t, &changeset[i], schFast)
	}
}

func TestStrictifyIntegerTypesNegativeLessThanLowerBound(t *testing.T) {
	schFast := changeitem.MakeFastTableSchema(integerTypesSch.Columns())
	template := makeChangeItemTemplate(integerTypesSch)

	changeset := []changeitem.ChangeItem{
		changeItemWithValues(template, []interface{}{
			int64(math.MinInt8 - 1), // i8
			uint64(0),               // u8
			int64(0),                // i16
			uint64(0),               // u16
			int64(0),                // i32
			uint64(0),               // u32
			int64(0),                // i64
			uint64(0),               // u64
		}),
		changeItemWithValues(template, []interface{}{
			int64(0),                 // i8
			uint64(0),                // u8
			int64(math.MinInt16 - 1), // i16
			uint64(0),                // u16
			int64(0),                 // i32
			uint64(0),                // u32
			int64(0),                 // i64
			uint64(0),                // u64
		}),
		changeItemWithValues(template, []interface{}{
			int64(0),                 // i8
			uint64(0),                // u8
			int64(0),                 // i16
			uint64(0),                // u16
			int64(math.MinInt32 - 1), // i32
			uint64(0),                // u32
			int64(0),                 // i64
			uint64(0),                // u64
		}),
		changeItemWithValues(template, []interface{}{
			int64(0),               // i8
			uint64(0),              // u8
			int64(0),               // i16
			uint64(0),              // u16
			int64(0),               // i32
			uint64(0),              // u32
			"-9223372036854775809", // i64 // workaround for unrepresentability of such a huge number
			uint64(0),              // u64
		}),
	}

	for i := range changeset {
		logger.Log.Infof("strictifying an item [%d]", i)
		executeNegativeStrictifyCheck(t, &changeset[i], schFast)
	}
}

var floatTypesSch = changeitem.NewTableSchema([]changeitem.ColSchema{
	{
		ColumnName: "f32",
		DataType:   schema.TypeFloat32.String(),
	},
	{
		ColumnName: "f64",
		DataType:   schema.TypeFloat64.String(),
	},
})

func TestStrictifyFloatTypesPositive(t *testing.T) {
	schFast := changeitem.MakeFastTableSchema(floatTypesSch.Columns())
	template := makeChangeItemTemplate(floatTypesSch)

	changeset := []changeitem.ChangeItem{
		changeItemWithValues(template, []interface{}{
			float32(0), // f32
			float64(0), // f64
		}),
		changeItemWithValues(template, []interface{}{
			float64(-1), // f32
			float32(-1), // f64
		}),
		changeItemWithValues(template, []interface{}{
			float64(math.SmallestNonzeroFloat32), // f32
			float64(math.SmallestNonzeroFloat64), // f64
		}),
		changeItemWithValues(template, []interface{}{
			float64(math.MaxFloat32), // f32
			float64(math.MaxFloat64), // f64
		}),
		changeItemWithValues(template, []interface{}{
			float64(1),            // f32
			json.Number("1.2345"), // f64
		}),
		changeItemWithValues(template, []interface{}{
			nil, // f32
			nil, // f64
		}),
	}

	for i := range changeset {
		logger.Log.Infof("strictifying an item [%d]", i)
		executePositiveStrictifyCheck(t, &changeset[i], schFast)
	}
}

func TestStrictifyFloatTypesNegativeWrongType(t *testing.T) {
	schFast := changeitem.MakeFastTableSchema(floatTypesSch.Columns())
	template := makeChangeItemTemplate(floatTypesSch)

	changeset := []changeitem.ChangeItem{
		changeItemWithValues(template, []interface{}{
			map[string]float32{"a": 1.0}, // f32
			float64(0),                   // f64
		}),
		changeItemWithValues(template, []interface{}{
			float32(0),                   // f32
			map[string]float64{"a": 1.0}, // f64
		}),
		changeItemWithValues(template, []interface{}{
			"wrong",    // f32
			float64(0), // f64
		}),
		changeItemWithValues(template, []interface{}{
			float32(0), // f32
			"wrong",    // f64
		}),
	}

	for i := range changeset {
		logger.Log.Infof("strictifying an item [%d]", i)
		executeNegativeStrictifyCheck(t, &changeset[i], schFast)
	}
}

var stringTypesSch = changeitem.NewTableSchema([]changeitem.ColSchema{
	{
		ColumnName: "s",
		DataType:   schema.TypeString.String(),
	},
	{
		ColumnName: "b",
		DataType:   schema.TypeBytes.String(),
	},
})

func TestStrictifyStringTypesPositive(t *testing.T) {
	schFast := changeitem.MakeFastTableSchema(stringTypesSch.Columns())
	template := makeChangeItemTemplate(stringTypesSch)

	changeset := []changeitem.ChangeItem{
		changeItemWithValues(template, []interface{}{
			"string", // s
			[]byte{0x73, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x0a}, // b
		}),
		changeItemWithValues(template, []interface{}{
			[]byte{0x73, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x0a}, // s
			"string", // b
		}),
		changeItemWithValues(template, []interface{}{
			[]byte{}, // s
			"",       // b
		}),
		changeItemWithValues(template, []interface{}{
			nil, // s
			nil, // b
		}),
	}

	for i := range changeset {
		logger.Log.Infof("strictifying an item [%d]", i)
		executePositiveStrictifyCheck(t, &changeset[i], schFast)
	}
}

func TestStrictifyStringTypesNegativeWrongType(t *testing.T) {
	schFast := changeitem.MakeFastTableSchema(stringTypesSch.Columns())
	template := makeChangeItemTemplate(stringTypesSch)

	changeset := []changeitem.ChangeItem{
		changeItemWithValues(template, []interface{}{
			map[string]string{"k": "v"},                      // s
			[]byte{0x73, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x0a}, // b
		}),
		changeItemWithValues(template, []interface{}{
			"string",                    // s
			map[string]string{"k": "v"}, // b
		}),
	}

	for i := range changeset {
		logger.Log.Infof("strictifying an item [%d]", i)
		executeNegativeStrictifyCheck(t, &changeset[i], schFast)
	}
}

var temporalTypesSch = changeitem.NewTableSchema([]changeitem.ColSchema{
	{
		ColumnName: "d",
		DataType:   schema.TypeDate.String(),
	},
	{
		ColumnName: "dt",
		DataType:   schema.TypeDatetime.String(),
	},
	{
		ColumnName: "ts",
		DataType:   schema.TypeTimestamp.String(),
	},
	{
		ColumnName: "i",
		DataType:   schema.TypeInterval.String(),
	},
})

func TestStrictifyTemporalTypesPositive(t *testing.T) {
	schFast := changeitem.MakeFastTableSchema(temporalTypesSch.Columns())
	template := makeChangeItemTemplate(temporalTypesSch)

	changeset := []changeitem.ChangeItem{
		changeItemWithValues(template, []interface{}{
			time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),      // d
			time.Date(1970, 1, 1, 1, 2, 3, 0, time.UTC),      // dt
			time.Date(1970, 1, 1, 1, 2, 3, 456789, time.UTC), // ts
			time.Second, // i
		}),
		changeItemWithValues(template, []interface{}{
			time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),      // d
			time.Date(1, 1, 1, 1, 2, 3, 0, time.UTC),      // dt
			time.Date(1, 1, 1, 1, 2, 3, 456789, time.UTC), // ts
			time.Nanosecond, // i
		}),
		changeItemWithValues(template, []interface{}{
			time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC),         // d
			time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC),      // dt
			time.Date(9999, 12, 31, 23, 59, 59, 999999, time.UTC), // ts
			48 * time.Hour, // i
		}),
		changeItemWithValues(template, []interface{}{
			nil, // d
			nil, // dt
			nil, // ts
			nil, // i
		}),
	}

	for i := range changeset {
		logger.Log.Infof("strictifying an item [%d]", i)
		executePositiveStrictifyCheck(t, &changeset[i], schFast)
	}
}

func TestStrictifyTemporalTypesNegativeWrongType(t *testing.T) {
	schFast := changeitem.MakeFastTableSchema(temporalTypesSch.Columns())
	template := makeChangeItemTemplate(temporalTypesSch)

	changeset := []changeitem.ChangeItem{
		changeItemWithValues(template, []interface{}{
			map[string]string{"k": "v"},                      // d
			time.Date(1970, 1, 1, 1, 2, 3, 0, time.UTC),      // dt
			time.Date(1970, 1, 1, 1, 2, 3, 456789, time.UTC), // ts
			time.Second, // i
		}),
		changeItemWithValues(template, []interface{}{
			time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),      // d
			map[string]string{"k": "v"},                      // dt
			time.Date(1970, 1, 1, 1, 2, 3, 456789, time.UTC), // ts
			time.Second, // i
		}),
		changeItemWithValues(template, []interface{}{
			time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), // d
			time.Date(1970, 1, 1, 1, 2, 3, 0, time.UTC), // dt
			map[string]string{"k": "v"},                 // ts
			time.Second,                                 // i
		}),
		changeItemWithValues(template, []interface{}{
			time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),      // d
			time.Date(1970, 1, 1, 1, 2, 3, 0, time.UTC),      // dt
			time.Date(1970, 1, 1, 1, 2, 3, 456789, time.UTC), // ts
			map[string]string{"k": "v"},                      // i
		}),
	}

	for i := range changeset {
		logger.Log.Infof("strictifying an item [%d]", i)
		executeNegativeStrictifyCheck(t, &changeset[i], schFast)
	}
}

var boolTypeSch = changeitem.NewTableSchema([]changeitem.ColSchema{
	{
		ColumnName: "b",
		DataType:   schema.TypeBoolean.String(),
	},
})

func TestStrictifyBoolTypePositive(t *testing.T) {
	schFast := changeitem.MakeFastTableSchema(boolTypeSch.Columns())
	template := makeChangeItemTemplate(boolTypeSch)

	changeset := []changeitem.ChangeItem{
		changeItemWithValues(template, []interface{}{
			true, // b
		}),
		changeItemWithValues(template, []interface{}{
			false, // b
		}),
		changeItemWithValues(template, []interface{}{
			nil, // b
		}),
	}

	for i := range changeset {
		logger.Log.Infof("strictifying an item [%d]", i)
		executePositiveStrictifyCheck(t, &changeset[i], schFast)
	}
}

func TestStrictifyBoolTypeNegativeWrongType(t *testing.T) {
	schFast := changeitem.MakeFastTableSchema(boolTypeSch.Columns())
	template := makeChangeItemTemplate(boolTypeSch)

	changeset := []changeitem.ChangeItem{
		changeItemWithValues(template, []interface{}{
			map[string]string{"k": "v"}, // b
		}),
	}

	for i := range changeset {
		logger.Log.Infof("strictifying an item [%d]", i)
		executeNegativeStrictifyCheck(t, &changeset[i], schFast)
	}
}
