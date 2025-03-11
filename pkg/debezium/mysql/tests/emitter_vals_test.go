package tests

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/library/go/test/yatest"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/debezium"
	debeziumparameters "github.com/transferria/transferria/pkg/debezium/parameters"
)

var mysqlDebeziumCanonizedValuesSnapshot = map[string]interface{}{
	"pk":               uint32(0x1),
	"bool1":            false,
	"bool2":            true,
	"bit":              true,
	"bit16":            "nwA=",
	"tinyint_":         int8(1),
	"tinyint_def":      int8(22),
	"tinyint_u":        uint8(255),
	"tinyint1":         true,
	"tinyint1u":        uint8(1),
	"smallint_":        int16(1000),
	"smallint5":        int16(100),
	"smallint_u":       uint16(10),
	"mediumint_":       int32(1),
	"mediumint5":       int32(11),
	"mediumint_u":      uint32(111),
	"int_":             int32(9),
	"integer_":         int32(99),
	"integer5":         int32(999),
	"int_u":            uint32(9999),
	"bigint_":          int64(8),
	"bigint5":          int64(88),
	"bigint_u":         int64(888),
	"real_":            123.45,
	"real_10_2":        99999.99,
	"float_":           1.23,
	"float_53":         1.23,
	"double_":          2.34,
	"double_precision": 2.34,
	"char_":            "a",
	"char5":            "abc",
	"varchar5":         "blab",
	"binary_":          "nw==",
	"binary5":          "nwAAAAA=",
	"varbinary5":       "n58=",
	"tinyblob_":        "n5+f",
	"tinytext_":        "qwerty12345",
	"blob_":            "/w==",
	"text_":            "my-text",
	"mediumblob_":      "q80=",
	"mediumtext_":      "my-mediumtext",
	"longblob_":        "q80=",
	"longtext_":        "my-longtext",
	"json_":            "{\"k1\":\"v1\"}",
	"enum_":            "x-small",
	"set_":             "a",
	"year_":            1901,
	"year4":            2155,
	"timestamp_":       "1999-01-01T00:00:01Z",
	"timestamp0":       "1999-10-19T10:23:54Z",
	"timestamp1":       "2004-10-19T10:23:54.1Z",
	"timestamp2":       "2004-10-19T10:23:54.12Z",
	"timestamp3":       "2004-10-19T10:23:54.123Z",
	"timestamp4":       "2004-10-19T10:23:54.1234Z",
	"timestamp5":       "2004-10-19T10:23:54.12345Z",
	"timestamp6":       "2004-10-19T10:23:54.123456Z",
	"date_":            int64(-354285),
	"time_":            uint64(14706000000),
	"time0":            uint64(14706000000),
	"time1":            uint64(14706100000),
	"time2":            uint64(14706120000),
	"time3":            uint64(14706123000),
	"time4":            uint64(14706123400),
	"time5":            uint64(14706123450),
	"time6":            uint64(14706123456),
	"datetime_":        uint64(1577891410000),
	"datetime0":        uint64(1577891410000),
	"datetime1":        uint64(1577891410100),
	"datetime2":        uint64(1577891410120),
	"datetime3":        uint64(1577891410123),
	"datetime4":        uint64(1577891410123400),
	"datetime5":        uint64(1577891410123450),
	"datetime6":        uint64(1577891410123456),
	"NUMERIC_":         "SZYC0g==",
	"NUMERIC_5":        "MDk=",
	"NUMERIC_5_2":      "MDk=",
	"DECIMAL_":         "AIvQODU=",
	"DECIMAL_5":        "W5s=",
	"DECIMAL_5_2":      "Wmk=",
}

func TestMysqlValByValInsert(t *testing.T) {
	mysqlSnapshotChangeItem, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/mysql/tests/testdata/emitter_vals_test__canon_change_item.txt"))
	require.NoError(t, err)

	changeItem, err := abstract.UnmarshalChangeItem(mysqlSnapshotChangeItem)
	require.NoError(t, err)

	params := debeziumparameters.EnrichedWithDefaults(map[string]string{debeziumparameters.TopicPrefix: "fullfillment"})
	afterVals, err := debezium.BuildKVMap(changeItem, params, true)
	require.NoError(t, err)

	require.Equal(t, len(mysqlDebeziumCanonizedValuesSnapshot), len(afterVals))
	for k, v := range afterVals {
		require.Equal(t, mysqlDebeziumCanonizedValuesSnapshot[k], v, k)
	}
}

func TestMysqlValByValInsertV8(t *testing.T) {
	mysqlSnapshotChangeItem, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/mysql/tests/testdata/emitter_vals_test__canon_change_item_v8.txt"))
	require.NoError(t, err)

	changeItem, err := abstract.UnmarshalChangeItem(mysqlSnapshotChangeItem)
	require.NoError(t, err)

	params := debeziumparameters.EnrichedWithDefaults(map[string]string{debeziumparameters.TopicPrefix: "fullfillment"})
	afterVals, err := debezium.BuildKVMap(changeItem, params, true)
	require.NoError(t, err)

	require.Equal(t, len(mysqlDebeziumCanonizedValuesSnapshot), len(afterVals))
	for k, v := range afterVals {
		require.Equal(t, mysqlDebeziumCanonizedValuesSnapshot[k], v, k)
	}
}
