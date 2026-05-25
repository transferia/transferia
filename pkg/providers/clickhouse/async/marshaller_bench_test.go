package async

import (
	"testing"
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse/columntypes"
)

// Схема и значения соответствуют medium_tbl_test.yson (zen-feed события).
// Типы выведены из YT-схемы: 36 int64, 17 utf8-строк, 9 bytes, 2 datetime, 5 any, 1 bool.
// Значения имитируют данные после YT Skiff → makeRowConverter:
//   - int64   → int64 (nullable → nil для пустых полей)
//   - string  → string (nullable → nil)
//   - bytes   → []byte  (IsString=true → string() conversion)
//   - datetime → time.Time (nativeCastField: time.Unix)
//   - any     → []any{} (yson.Unmarshal пустого массива)
//   - boolean → nil (поле пустое в тестовых данных)

var benchRow, benchSchema, benchCols = buildMediumTblBench()

func buildMediumTblBench() (abstract.ChangeItem, map[string]abstract.ColSchema, columntypes.TypeMapping) {
	type col struct {
		name   string
		chType string // для TypeMapping
		ytType string // DataType в ColSchema (YT type string)
		value  any
	}

	columns := []col{
		// int64 (36) — большинство nullable, пустые строки → int64(0)
		{"adProviderId", "Int64", "int64", int64(0)},
		{"apiNameId", "Int64", "int64", int64(0)},
		{"clickOnComment", "Int64", "int64", int64(0)},
		{"eventId", "Int64", "int64", int64(42)},
		{"flightId", "Int64", "int64", int64(0)},
		{"integrationId", "Int64", "int64", int64(7)},
		{"isFavouriteSource", "Int64", "int64", int64(0)},
		{"isMobile", "Int64", "int64", int64(1)},
		{"isRobot", "Int64", "int64", int64(0)},
		{"isShort", "Int64", "int64", int64(0)},
		{"isTablet", "Int64", "int64", int64(0)},
		{"itemId", "Int64", "int64", int64(0)},
		{"itemTypeId", "Int64", "int64", int64(3)},
		{"loadTimeMs", "Int64", "int64", int64(0)},
		{"pageTypeId", "Int64", "int64", int64(0)},
		{"parentItemId", "Int64", "int64", int64(0)},
		{"parentItemTypeId", "Int64", "int64", int64(0)},
		{"parentSize", "Int64", "int64", int64(0)},
		{"partnerIdTypeId", "Int64", "int64", int64(0)},
		{"placeId", "Int64", "int64", int64(0)},
		{"pos", "Int64", "int64", int64(0)},
		{"prestableFlag", "Int64", "int64", int64(0)},
		{"referrerIntegrationId", "Int64", "int64", int64(0)},
		{"referrerPartnerId", "Int64", "int64", int64(0)},
		{"requestTimeMs", "Int64", "int64", int64(0)},
		{"scrollPercent", "Int64", "int64", int64(0)},
		{"sourceItemId", "Int64", "int64", int64(0)},
		{"sourceItemTypeId", "Int64", "int64", int64(0)},
		{"strongestId", "Int64", "int64", int64(1480278141)},
		{"userClicksCount", "Int64", "int64", int64(0)},
		{"userIdTypeId", "Int64", "int64", int64(0)},
		{"userVisitsCount", "Int64", "int64", int64(5)},
		{"variantId", "Int64", "int64", int64(0)},
		{"viewTimeSec", "Int64", "int64", int64(0)},
		{"viewTimeTillEndSec", "Int64", "int64", int64(0)},
		{"withCommentId", "Int64", "int64", int64(0)},

		// string (17) — utf8; TypeString="utf8"; nullable → nil для пустых
		// DataType "utf8" (ytschema.TypeString = "utf8")
		{"browserEngine", "String", "utf8", nil},
		{"browserEngineVersion", "String", "utf8", "undefined"},
		{"browserName", "String", "utf8", "zenkit"},
		{"browserVersion", "String", "utf8", "22.1.6.0.1913325346"},
		{"countryCode", "String", "utf8", "ru"},
		{"dc", "String", "utf8", "vla"},
		{"osFamily", "String", "utf8", "Android"},
		{"osVersion", "String", "utf8", "10"},
		{"partnerUserId", "String", "utf8", nil},
		{"productId", "String", "utf8", nil},
		{"recHostId", "String", "utf8", nil},
		{"recVersion", "String", "utf8", "64.3.408bdc099f48"},
		{"referrerProductId", "String", "utf8", nil},
		{"sfId", "String", "utf8", nil},
		{"url", "String", "utf8", nil},
		{"xurmaSign", "String", "utf8", nil},
		{"zenBranchId", "String", "utf8", nil},

		// bytes (9) — TypeBytes="string"; CH-тип String (IsString=true → []byte→string)
		// DataType "string" (ytschema.TypeBytes = "string")
		{"clid", "String", "string", nil},
		{"deviceId", "String", "string", []byte("627e27e8592c333d60386981edd92cae")},
		{"partnerId", "String", "string", []byte("R")},
		{"rid1", "String", "string", []byte{0x96, 0x83, 0xe2, 0xad}},
		{"rid2", "String", "string", []byte("V")},
		{"rid3", "String", "string", []byte{0xad, 0xe2, 0xad, 0x5f}},
		{"rid4", "String", "string", []byte{0x96, 0xa8}},
		{"uid", "String", "string", []byte{0xce, 0xc8, 0xda, 0x83}},
		{"yandexuid", "String", "string", []byte{0xce, 0x93, 0x9e, 0xde, 0xba}},

		// datetime (2) — приходит как time.Time после nativeCastField (time.Unix)
		// DataType "datetime" (ytschema.TypeDatetime = "datetime")
		{"clientTimestamp", "DateTime", "datetime", time.Unix(1646141338, 0).UTC()},
		{"timestamp", "DateTime", "datetime", time.Unix(1646144941, 0).UTC()},

		// any (5) — TypeAny="any"; yson.Unmarshal для пустых массивов → []any{}
		// DataType "any" (ytschema.TypeAny = "any")
		{"experiments", "String", "any", []any{}},
		{"geoPath", "String", "any", []any{int64(1), int64(2), int64(4)}},
		{"recExperiments", "String", "any", []any{}},
		{"urlType", "String", "any", []any{}},
		{"xurmaRules", "String", "any", []any{}},

		// bool (1) — nullable, пустое в данных → nil
		// DataType "boolean" (ytschema.TypeBoolean = "boolean")
		{"staff", "UInt8", "boolean", nil},
	}

	names := make([]string, len(columns))
	values := make([]any, len(columns))
	cols := make(columntypes.TypeMapping, len(columns))
	schema := make(map[string]abstract.ColSchema, len(columns))

	for i, c := range columns {
		names[i] = c.name
		values[i] = c.value
		cols[c.name] = columntypes.NewTypeDescription(c.chType)
		schema[c.name] = abstract.ColSchema{ColumnName: c.name, DataType: c.ytType}
	}

	return abstract.ChangeItem{
		ColumnNames:  names,
		ColumnValues: values,
	}, schema, cols
}

func BenchmarkMarshalChangeItem(b *testing.B) {
	m := &chV2Marshaller{schema: benchSchema, cols: benchCols}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = m.marshal(benchRow)
	}
}
