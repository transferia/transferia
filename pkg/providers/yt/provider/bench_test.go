package provider

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	yt_table "github.com/transferia/transferia/pkg/providers/yt/provider/table"
	yt_provider_types "github.com/transferia/transferia/pkg/providers/yt/provider/types"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/skiff"
)

type benchColSpec struct {
	name     string
	ytType   ytschema.Type
	nullable bool
}

var benchColSpecs = []benchColSpec{
	{"adProviderId", ytschema.TypeInt64, true},
	{"apiNameId", ytschema.TypeInt64, true},
	{"clickOnComment", ytschema.TypeInt64, true},
	{"eventId", ytschema.TypeInt64, true},
	{"flightId", ytschema.TypeInt64, true},
	{"integrationId", ytschema.TypeInt64, true},
	{"isFavouriteSource", ytschema.TypeInt64, true},
	{"isMobile", ytschema.TypeInt64, true},
	{"isRobot", ytschema.TypeInt64, true},
	{"isShort", ytschema.TypeInt64, true},
	{"isTablet", ytschema.TypeInt64, true},
	{"itemId", ytschema.TypeInt64, true},
	{"itemTypeId", ytschema.TypeInt64, true},
	{"loadTimeMs", ytschema.TypeInt64, true},
	{"pageTypeId", ytschema.TypeInt64, true},
	{"parentItemId", ytschema.TypeInt64, true},
	{"parentItemTypeId", ytschema.TypeInt64, true},
	{"parentSize", ytschema.TypeInt64, true},
	{"partnerIdTypeId", ytschema.TypeInt64, true},
	{"placeId", ytschema.TypeInt64, true},
	{"pos", ytschema.TypeInt64, true},
	{"prestableFlag", ytschema.TypeInt64, true},
	{"referrerIntegrationId", ytschema.TypeInt64, true},
	{"referrerPartnerId", ytschema.TypeInt64, true},
	{"requestTimeMs", ytschema.TypeInt64, true},
	{"scrollPercent", ytschema.TypeInt64, true},
	{"sourceItemId", ytschema.TypeInt64, true},
	{"sourceItemTypeId", ytschema.TypeInt64, true},
	{"strongestId", ytschema.TypeInt64, false},
	{"userClicksCount", ytschema.TypeInt64, true},
	{"userIdTypeId", ytschema.TypeInt64, true},
	{"userVisitsCount", ytschema.TypeInt64, true},
	{"variantId", ytschema.TypeInt64, true},
	{"viewTimeSec", ytschema.TypeInt64, true},
	{"viewTimeTillEndSec", ytschema.TypeInt64, true},
	{"withCommentId", ytschema.TypeInt64, true},

	{"browserEngine", ytschema.TypeString, true},
	{"browserEngineVersion", ytschema.TypeString, true},
	{"browserName", ytschema.TypeString, false},
	{"browserVersion", ytschema.TypeString, false},
	{"countryCode", ytschema.TypeString, false},
	{"dc", ytschema.TypeString, false},
	{"osFamily", ytschema.TypeString, false},
	{"osVersion", ytschema.TypeString, false},
	{"partnerUserId", ytschema.TypeString, true},
	{"productId", ytschema.TypeString, true},
	{"recHostId", ytschema.TypeString, true},
	{"recVersion", ytschema.TypeString, false},
	{"referrerProductId", ytschema.TypeString, true},
	{"sfId", ytschema.TypeString, true},
	{"url", ytschema.TypeString, true},
	{"xurmaSign", ytschema.TypeString, true},
	{"zenBranchId", ytschema.TypeString, true},

	{"clid", ytschema.TypeBytes, true},
	{"deviceId", ytschema.TypeBytes, true},
	{"partnerId", ytschema.TypeBytes, true},
	{"rid1", ytschema.TypeBytes, true},
	{"rid2", ytschema.TypeBytes, true},
	{"rid3", ytschema.TypeBytes, true},
	{"rid4", ytschema.TypeBytes, true},
	{"uid", ytschema.TypeBytes, true},
	{"yandexuid", ytschema.TypeBytes, true},

	{"clientTimestamp", ytschema.TypeDatetime, false},
	{"timestamp", ytschema.TypeDatetime, false},

	{"experiments", ytschema.TypeAny, false},
	{"geoPath", ytschema.TypeAny, false},
	{"recExperiments", ytschema.TypeAny, false},
	{"urlType", ytschema.TypeAny, false},
	{"xurmaRules", ytschema.TypeAny, false},

	{"staff", ytschema.TypeBoolean, true},
}

func benchTable(nCols int) yt_table.YtTable {
	if nCols > len(benchColSpecs) {
		nCols = len(benchColSpecs)
	}
	tbl := yt_table.NewTable("bench_table")
	for _, spec := range benchColSpecs[:nCols] {
		abstractType, err := yt_provider_types.Resolve(spec.ytType)
		if err != nil {
			panic(fmt.Sprintf("resolve type for %s: %v", spec.name, err))
		}
		col := yt_table.NewColumn(
			spec.name,
			abstractType,
			spec.ytType,
			ytschema.Column{Name: spec.name, Type: spec.ytType, Required: !spec.nullable},
			spec.nullable,
		)
		tbl.AddColumn(col)
	}
	return tbl
}

func benchRowMap() map[string]interface{} {
	m := make(map[string]interface{}, len(benchColSpecs))
	for _, spec := range benchColSpecs {
		switch spec.ytType {
		case ytschema.TypeInt64:
			switch spec.name {
			case "strongestId":
				m[spec.name] = int64(1480278141)
			case "userVisitsCount":
				m[spec.name] = int64(5)
			case "itemTypeId":
				m[spec.name] = int64(3)
			default:
				m[spec.name] = int64(0)
			}
		case ytschema.TypeString:
			switch spec.name {
			case "browserName":
				m[spec.name] = "zenkit"
			case "browserVersion":
				m[spec.name] = "22.1.6.0.1913325346"
			case "countryCode":
				m[spec.name] = "ru"
			case "dc":
				m[spec.name] = "vla"
			case "osFamily":
				m[spec.name] = "Android"
			case "osVersion":
				m[spec.name] = "10"
			case "recVersion":
				m[spec.name] = "64.3.408bdc099f48"
			case "browserEngineVersion":
				m[spec.name] = "undefined"
			default:
				m[spec.name] = nil
			}
		case ytschema.TypeBytes:
			switch spec.name {
			case "deviceId":
				m[spec.name] = []byte("627e27e8592c333d60386981edd92cae")
			case "uid":
				m[spec.name] = []byte{0xce, 0xc8, 0xda, 0x83}
			case "yandexuid":
				m[spec.name] = []byte{0xce, 0x93, 0x9e, 0xde, 0xba}
			case "rid1":
				m[spec.name] = []byte{0x96, 0x83, 0xe2, 0xad}
			case "rid2":
				m[spec.name] = []byte("V")
			case "rid3":
				m[spec.name] = []byte{0xad, 0xe2, 0xad, 0x5f}
			case "rid4":
				m[spec.name] = []byte{0x96, 0xa8}
			default:
				m[spec.name] = nil
			}
		case ytschema.TypeDatetime:
			m[spec.name] = uint64(1646144941)
		case ytschema.TypeAny:
			m[spec.name] = []interface{}{}
		case ytschema.TypeBoolean:
			m[spec.name] = nil
		}
	}
	return m
}

func benchSkiffBytes(tbl yt_table.YtTable, n int) ([]byte, skiff.Format) {
	ytSchema := ytSchemaForSkiff(tbl, "")
	skiffSchema := skiff.FromTableSchema(ytSchema)

	var buf bytes.Buffer
	enc, err := skiff.NewEncoder(&buf, skiffSchema)
	if err != nil {
		panic(err)
	}
	row := benchRowMap()
	for i := 0; i < n; i++ {
		if err := enc.Write(row); err != nil {
			panic(err)
		}
	}
	if err := enc.Flush(); err != nil {
		panic(err)
	}
	return buf.Bytes(), skiff.Format{Name: "skiff", TableSchemas: []any{&skiffSchema}}
}

// Skiff binary → reflect.StructOf decode → makeRowConverter → ChangeItem.
func BenchmarkSkiffNativeTypes(b *testing.B) {
	for _, nCols := range []int{10, 25, 70} {
		b.Run(fmt.Sprintf("cols=%d", nCols), func(b *testing.B) {
			tbl := benchTable(nCols)
			skiffData, skiffFmt := benchSkiffBytes(tbl, 100)
			rowType := buildSkiffRowType(tbl, "")
			converter := makeRowConverter(tbl, "")
			rowPtr := reflect.New(rowType)

			b.ResetTimer()
			b.ReportAllocs()
			rowIdx := int64(0)
			for i := 0; i < b.N; i++ {
				dec, err := skiff.NewDecoder(bytes.NewReader(skiffData), skiffFmt)
				if err != nil {
					b.Fatal(err)
				}
				for dec.Next() {
					rowPtr.Elem().Set(reflect.Zero(rowType))
					if err := dec.Scan(rowPtr.Interface()); err != nil {
						b.Fatal(err)
					}
					_, err := converter(rowPtr.Elem(), rowIdx)
					if err != nil {
						b.Fatal(err)
					}
					rowIdx++
				}
				if err := dec.Err(); err != nil {
					b.Fatal(err)
				}
			}
			b.SetBytes(int64(len(skiffData)))
		})
	}
}
