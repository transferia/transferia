package tests

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/library/go/test/yatest"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/debezium"
	debeziumparameters "github.com/transferria/transferria/pkg/debezium/parameters"
)

var ydbDebeziumCanonizedValuesSnapshot = map[string]interface{}{
	"id": int64(0x1),

	"Bool_":         true,
	"Int8_":         int8(1),
	"Int16_":        int16(2),
	"Int32_":        int32(3),
	"Int64_":        int64(4),
	"Uint8_":        uint8(5),
	"Uint16_":       uint16(6),
	"Uint32_":       uint32(7),
	"Uint64_":       int64(8),
	"Float_":        json.Number("1.1"),
	"Double_":       json.Number("2.2"),
	"Decimal_":      "Nnt8pAA=",
	"DyNumber_":     map[string]interface{}{"scale": 0, "value": "ew=="},
	"String_":       []byte{1},
	"Utf8_":         "my_utf8_string",
	"Json_":         "{}",
	"JsonDocument_": "{}",
	"Date_":         int32(18294),
	"Datetime_":     int64(1580637742000),
	"Timestamp_":    int64(1580637742000000),
	"Interval_":     time.Duration(123000),
}

func TestYDBValByValInsert(t *testing.T) {
	changeItemStr, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/pkg/debezium/ydb/tests/testdata/emitter_vals_test__canon_change_item.txt"))
	require.NoError(t, err)

	changeItem, err := abstract.UnmarshalChangeItem(changeItemStr)
	require.NoError(t, err)

	params := debeziumparameters.EnrichedWithDefaults(map[string]string{debeziumparameters.DatabaseDBName: "pguser", debeziumparameters.TopicPrefix: "fullfillment"})
	afterVals, err := debezium.BuildKVMap(changeItem, params, true)
	require.NoError(t, err)

	require.Equal(t, len(ydbDebeziumCanonizedValuesSnapshot), len(afterVals))
	for k, v := range afterVals {
		require.Equal(t, ydbDebeziumCanonizedValuesSnapshot[k], v)
	}
}
