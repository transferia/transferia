package tests

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/test/yatest"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/debezium"
	debeziumcommon "github.com/transferria/transferria/pkg/debezium/common"
	debeziumparameters "github.com/transferria/transferria/pkg/debezium/parameters"
	"github.com/transferria/transferria/pkg/debezium/testutil"
	"github.com/transferria/transferria/pkg/debezium/typeutil"
)

var pgDebeziumCanonizedValuesSnapshot = map[string]interface{}{
	"bl":                   true,
	"b":                    true,
	"b8":                   "rw==",
	"vb":                   "rg==",
	"si":                   -32768,
	"ss":                   1,
	"int":                  -8388605,
	"aid":                  0,
	"id":                   int64(1),
	"bid":                  int64(3372036854775807),
	"oid_":                 int64(2),
	"real_":                float32(1.45e-10),
	"d":                    3.14e-100,
	"c":                    "1",
	"str":                  "varchar_example",
	"character_":           "abcd",
	"character_varying_":   "varc",
	"timestamptz_":         "2004-10-19T08:23:54Z",
	"tst":                  "2004-10-19T09:23:54Z",
	"timetz_":              "08:51:02.746572Z",
	"time_with_time_zone_": "08:51:02.746572Z",
	"iv":                   uint64(90000000000),
	"ba":                   "yv66vg==",
	"j":                    "{\"k1\":\"v1\"}",
	"jb":                   "{\"k2\":\"v2\"}",
	"x":                    "<foo>bar</foo>",
	"uid":                  "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
	"pt": map[string]interface{}{
		"x":    23.4,
		"y":    -44.5,
		"wkb":  "",
		"srid": nil,
	},
	"it":         "192.168.100.128/25",
	"int4range_": "[3,7)",
	"int8range_": "[3,7)",
	"numrange_":  "[1.9,1.91)",
	"tsrange_":   "[\"2010-01-02 10:00:00\",\"2010-01-02 11:00:00\")",
	"tstzrange_": "[\"2010-01-01 06:00:00+00\",\"2010-01-01 10:00:00+00\")",
	"daterange_": "[2000-01-10,2000-01-21)",
	"f":          1.45e-10,
	"i":          1,
	"t":          "text_example",

	"date_": 10599,
	"time_": uint64(14706000000),
	"time1": uint64(14706100),

	//---
	"time6":      uint64(14706123456),
	"timetz__":   "17:30:25Z",
	"timetz1":    "17:30:25.5Z",
	"timetz6":    "17:30:25.575401Z",
	"timestamp1": uint64(1098181434900),
	"timestamp6": uint64(1098181434987654),
	"timestamp":  uint64(1098181434000000),
	"numeric_": map[string]interface{}{
		"scale": 0,
		"value": "EAAAAAAAAAAAAAAAAA==",
	},
	"numeric_5":   "MDk=",
	"numeric_5_2": "ME8=",
	"decimal_": map[string]interface{}{
		"scale": 0,
		"value": "AeJA",
	},
	"decimal_5":   "MDk=",
	"decimal_5_2": "ME8=",
	"money_":      "Jw4=",
	"hstore_":     "{\"a\":\"1\",\"b\":\"2\"}",
	"inet_":       "192.168.1.5",
	"cidr_":       "10.1.0.0/16",
	"macaddr_":    "08:00:2b:01:02:03",
	"citext_":     "Tom",
}

func TestPgValByValInsert(t *testing.T) {
	pgSnapshotChangeItem, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_vals_test__canon_change_item.txt"))
	require.NoError(t, err)

	changeItem, err := abstract.UnmarshalChangeItem(pgSnapshotChangeItem)
	require.NoError(t, err)

	params := debeziumparameters.EnrichedWithDefaults(map[string]string{debeziumparameters.DatabaseDBName: "pguser", debeziumparameters.TopicPrefix: "fullfillment"})
	afterVals, err := debezium.BuildKVMap(changeItem, params, true)
	require.NoError(t, err)

	require.Equal(t, len(pgDebeziumCanonizedValuesSnapshot), len(afterVals))
	for k, v := range afterVals {
		require.Equal(t, pgDebeziumCanonizedValuesSnapshot[k], v)
	}
}

var pgDebeziumCanonizedArrSnapshot = map[string]interface{}{
	"i":                        1,
	"arr_bl":                   []interface{}{true, true},
	"arr_si":                   []interface{}{1, 2},
	"arr_int":                  []interface{}{1, 2},
	"arr_id":                   []interface{}{int64(1), int64(2)},
	"arr_oid_":                 []interface{}{int64(1), int64(2)},
	"arr_real_":                []interface{}{float32(1.45e-10), float32(1.45e-10)},
	"arr_d":                    []interface{}{3.14e-100, 3.14e-100},
	"arr_c":                    []interface{}{"1", "1"},
	"arr_str":                  []interface{}{"varchar_example", "varchar_example"},
	"arr_character_":           []interface{}{"abcd", "abcd"},
	"arr_character_varying_":   []interface{}{"varc", "varc"},
	"arr_timestamptz_":         []interface{}{"2004-10-19T08:23:54Z", "2004-10-19T08:23:54Z"},
	"arr_tst":                  []interface{}{"2004-10-19T09:23:54Z", "2004-10-19T09:23:54Z"},
	"arr_timetz_":              []interface{}{"08:51:02Z", "08:51:02Z"},
	"arr_time_with_time_zone_": []interface{}{"08:51:02Z", "08:51:02Z"},
	"arr_uid":                  []interface{}{"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
	"arr_it":                   []interface{}{"192.168.100.128/25", "192.168.100.128/25"},
	"arr_f":                    []interface{}{1.45e-10, 1.45e-10},
	"arr_i":                    []interface{}{1, 1},
	"arr_t":                    []interface{}{"text_example", "text_example"},
	"arr_date_":                []interface{}{10599, 10599},
	"arr_time_":                []interface{}{uint64(14706000000), uint64(14706000000)},
	"arr_time1":                []interface{}{uint64(14706100000), uint64(14706100000)},
	"arr_time6":                []interface{}{uint64(14706123000), uint64(14706123000)},
	"arr_timetz__":             []interface{}{"17:30:25Z", "17:30:25Z"},
	"arr_timetz1":              []interface{}{"17:30:25Z", "17:30:25Z"},
	"arr_timetz6":              []interface{}{"17:30:25Z", "17:30:25Z"},
	"arr_timestamp1":           []interface{}{uint64(1098181434900000), uint64(1098181434900000)},
	"arr_timestamp6":           []interface{}{uint64(1098181434987654), uint64(1098181434987654)},
	"arr_timestamp":            []interface{}{uint64(1098181434000000), uint64(1098181434000000)},
	"arr_numeric_": []interface{}{
		map[string]interface{}{
			"scale": 0,
			"value": "EAAAAAAAAAAAAAAAAA==",
		},
		map[string]interface{}{
			"scale": 14,
			"value": "EAAAAAAAAAAAAAAAAA==",
		},
	},
	"arr_numeric_5":   []interface{}{"MDk=", "MDk="},
	"arr_numeric_5_2": []interface{}{"ME8=", "ME8="},
	"arr_decimal_": []interface{}{
		map[string]interface{}{
			"scale": 0,
			"value": "AeJA",
		},
		map[string]interface{}{
			"scale": 0,
			"value": "AeJA",
		},
	},
	"arr_decimal_5":   []interface{}{"MDk=", "MDk="},
	"arr_decimal_5_2": []interface{}{"ME8=", "ME8="},
}

func TestPgArrByArrInsert(t *testing.T) {
	pgSnapshotChangeItem, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_vals_test__canon_change_item_arr.txt"))
	require.NoError(t, err)

	changeItem, err := abstract.UnmarshalChangeItem(pgSnapshotChangeItem)
	require.NoError(t, err)

	params := debeziumparameters.EnrichedWithDefaults(map[string]string{debeziumparameters.DatabaseDBName: "pguser", debeziumparameters.TopicPrefix: "fullfillment"})
	afterVals, err := debezium.BuildKVMap(changeItem, params, true)
	require.NoError(t, err)

	require.Equal(t, len(pgDebeziumCanonizedArrSnapshot), len(afterVals))
	for k, v := range afterVals {
		require.Equal(t, pgDebeziumCanonizedArrSnapshot[k], v)
	}
}

func TestPgWholeJSONSnapshot(t *testing.T) {
	// take original 'after' from debezium postgres->kafka tutorial in docker
	// deal with last element comma
	// sort
	// replace wkb value in pt by "", reorder fields in pt alphabetical (srid-wkb-x-y), and make pt dict in one-line
	// remove \n and spaces between key & value
	canonizedDebeziumAfterJSONInsert, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_vals_test__canon_after.txt"))
	require.NoError(t, err)

	changeItemStr, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_vals_test__canon_change_item.txt"))
	require.NoError(t, err)

	changeItem, err := abstract.UnmarshalChangeItem(changeItemStr)
	require.NoError(t, err)

	params := debeziumparameters.EnrichedWithDefaults(map[string]string{debeziumparameters.DatabaseDBName: "pguser", debeziumparameters.TopicPrefix: "fullfillment"})
	afterVals, err := debezium.BuildKVMap(changeItem, params, true)
	require.NoError(t, err)

	syntheticDebeziumAfterJSON, err := json.Marshal(afterVals)
	require.NoError(t, err)

	// deal with xml - json.Marshal escaped '<' and '>'
	syntheticDebeziumAfterJSONStrFinal := typeutil.UnescapeUnicode(string(syntheticDebeziumAfterJSON))

	testutil.Compare(t, string(canonizedDebeziumAfterJSONInsert), syntheticDebeziumAfterJSONStrFinal+"\n")
}

func TestPgUserDefinedType(t *testing.T) {
	changeItemStr, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_vals_test__change_item_with_user_defined_type.txt"))
	require.NoError(t, err)
	changeItem, err := abstract.UnmarshalChangeItem(changeItemStr)
	require.NoError(t, err)

	// default behaviour: fail
	params1 := debeziumparameters.EnrichedWithDefaults(map[string]string{debeziumparameters.DatabaseDBName: "pguser", debeziumparameters.TopicPrefix: "fullfillment"})
	_, err = debezium.BuildKVMap(changeItem, params1, true)
	require.Error(t, err)
	require.True(t, debeziumcommon.IsUnknownTypeError(err))

	emitter1, err := debezium.NewMessagesEmitter(map[string]string{debeziumparameters.DatabaseDBName: "public", debeziumparameters.TopicPrefix: "my_topic"}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	_, err = emitter1.EmitKV(changeItem, time.Time{}, true, nil)
	require.Error(t, err)
	require.True(t, debeziumcommon.IsUnknownTypeError(err))

	// behaviour: skip
	params2 := debeziumparameters.EnrichedWithDefaults(map[string]string{debeziumparameters.DatabaseDBName: "pguser", debeziumparameters.TopicPrefix: "fullfillment", debeziumparameters.UnknownTypesPolicy: debeziumparameters.UnknownTypesPolicySkip})
	afterVals, err := debezium.BuildKVMap(changeItem, params2, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(afterVals))

	emitter2, err := debezium.NewMessagesEmitter(map[string]string{debeziumparameters.DatabaseDBName: "pguser", debeziumparameters.TopicPrefix: "fullfillment", debeziumparameters.UnknownTypesPolicy: debeziumparameters.UnknownTypesPolicySkip}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	resultKV, err := emitter2.EmitKV(changeItem, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(resultKV))
}
