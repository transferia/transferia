package tests

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/debezium"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
)

func TestTxInfo(t *testing.T) {
	changeItemStr := `
	{
		"id": 0,
		"nextlsn": 2000000013747,
		"commitTime": 0,
		"txPosition": 0,
		"kind": "insert",
		"schema": "",
		"table": "customers3",
		"columnnames": ["pk", "bigint_u"],
		"columnvalues": [2, 18446744073709551615],
		"table_schema": [{
			"path": "",
			"name": "pk",
			"type": "uint32",
			"key": true,
			"required": false,
			"original_type": "mysql:int(10) unsigned"
		}, {
			"path": "",
			"name": "bigint_u",
			"type": "uint64",
			"key": false,
			"required": false,
			"original_type": "mysql:bigint(20) unsigned"
		}],
		"oldkeys": {},
		"tx_id": "58c4f6fc-27b5-11ed-b434-0242ac1e0002:2",
		"query": ""
	}`
	changeItem, err := abstract.UnmarshalChangeItem([]byte(changeItemStr))
	require.NoError(t, err)

	// check if conversation works

	params := map[string]string{
		debeziumparameters.TopicPrefix:      "my_topic",
		debeziumparameters.AddOriginalTypes: "true",
		debeziumparameters.SourceType:       "mysql",
	}
	emitter, err := debezium.NewMessagesEmitter(params, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	emitter.TestSetIgnoreUnknownSources(true)
	currDebeziumKV, err := emitter.EmitKV(changeItem, time.Time{}, true, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(currDebeziumKV))

	// check meta info

	msg := *currDebeziumKV[0].DebeziumVal
	require.True(t, strings.Contains(msg, `"file":"mysql-log.000002"`))
	require.True(t, strings.Contains(msg, `"pos":13747`))
	require.True(t, strings.Contains(msg, `"gtid":"58c4f6fc-27b5-11ed-b434-0242ac1e0002:2"`))
}
