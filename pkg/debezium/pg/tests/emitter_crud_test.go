package tests

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/library/go/test/yatest"
	"github.com/transferria/transferria/pkg/abstract"
	debeziumcommon "github.com/transferria/transferria/pkg/debezium/common"
	"github.com/transferria/transferria/pkg/debezium/testutil"
)

var (
	insert  *abstract.ChangeItem
	update0 *abstract.ChangeItem
	update1 *abstract.ChangeItem
	update2 *abstract.ChangeItem
	delete_ *abstract.ChangeItem

	insertDebebziumVal  string
	update0DebeziumVal  string
	update1DebeziumVal  string
	update2DebeziumVal0 string
	update2DebeziumVal2 string
	deleteDebeziumVal   string
)

func init() {
	insertDebebziumVal = ``
	update0DebeziumVal = ``
	update1DebeziumVal = ``
	update2DebeziumVal0 = ``
	update2DebeziumVal2 = ``
	deleteDebeziumVal = ``
}

func InitCanon(t *testing.T) {
	changeItems := []**abstract.ChangeItem{
		&insert,
		&update0,
		&update1,
		&update2,
		&delete_,
	}
	pathsToChangeItem := []string{
		"transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_crud_test__insert.txt",
		"transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_crud_test__update0.txt",
		"transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_crud_test__update1.txt",
		"transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_crud_test__update2.txt",
		"transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_crud_test__delete.txt",
	}
	for i, path := range pathsToChangeItem {
		insertBytes, err := os.ReadFile(yatest.SourcePath(path))
		require.NoError(t, err)
		changeItem, err := abstract.UnmarshalChangeItem(insertBytes)
		require.NoError(t, err)
		*changeItems[i] = changeItem
	}
	//--------------------------------------------------------------
	debeziumMsgs := []*string{
		&insertDebebziumVal,
		&update0DebeziumVal,
		&update1DebeziumVal,
		&update2DebeziumVal0,
		&update2DebeziumVal2,
		&deleteDebeziumVal,
	}
	pathsToDebeziumCanonPaths := []string{
		"transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_crud_test__debezium_insert.txt",
		"transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_crud_test__debezium_update0val.txt",
		"transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_crud_test__debezium_update1val.txt",
		"transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_crud_test__debezium_update2val0.txt",
		"transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_crud_test__debezium_update2val2.txt",
		"transfer_manager/go/pkg/debezium/pg/tests/testdata/emitter_crud_test__debezium_delete.txt",
	}
	for i, path := range pathsToDebeziumCanonPaths {
		currDebeziumMsg, err := os.ReadFile(yatest.SourcePath(path))
		require.NoError(t, err)
		*debeziumMsgs[i] = string(currDebeziumMsg)
	}
}

func getArrChangeItemCanon() []debeziumcommon.ChangeItemCanon {
	var changeItemCanonInsert = debeziumcommon.ChangeItemCanon{
		ChangeItem: insert,
		DebeziumEvents: []debeziumcommon.KeyValue{
			{
				DebeziumKey: `{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"i"}],"optional":false,"name":"fullfillment.public.basic_types.Key"},"payload":{"i":1}}`,
				DebeziumVal: &insertDebebziumVal,
			},
		},
	}

	var changeItemCanonUpdate1 = debeziumcommon.ChangeItemCanon{
		ChangeItem: update0,
		DebeziumEvents: []debeziumcommon.KeyValue{
			{
				DebeziumKey: `{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"i"}],"optional":false,"name":"fullfillment.public.basic_types.Key"},"payload":{"i":1}}`,
				DebeziumVal: &update0DebeziumVal,
			},
		},
	}

	var changeItemCanonUpdate2 = debeziumcommon.ChangeItemCanon{
		ChangeItem: update1,
		DebeziumEvents: []debeziumcommon.KeyValue{
			{
				DebeziumKey: `{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"i"}],"optional":false,"name":"fullfillment.public.basic_types.Key"},"payload":{"i":1}}`,
				DebeziumVal: &update1DebeziumVal,
			},
		},
	}

	var changeItemCanonUpdate3 = debeziumcommon.ChangeItemCanon{
		ChangeItem: update2,
		DebeziumEvents: []debeziumcommon.KeyValue{
			{
				DebeziumKey: `{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"i"}],"optional":false,"name":"fullfillment.public.basic_types.Key"},"payload":{"i":1}}`,
				DebeziumVal: &update2DebeziumVal0,
			},
			{
				DebeziumKey: `{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"i"}],"optional":false,"name":"fullfillment.public.basic_types.Key"},"payload":{"i":1}}`,
				DebeziumVal: nil,
			},
			{
				DebeziumKey: `{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"i"}],"optional":false,"name":"fullfillment.public.basic_types.Key"},"payload":{"i":2}}`,
				DebeziumVal: &update2DebeziumVal2,
			},
		},
	}

	var changeItemCanonDelete = debeziumcommon.ChangeItemCanon{
		ChangeItem: delete_,
		DebeziumEvents: []debeziumcommon.KeyValue{
			{
				DebeziumKey: `{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"i"}],"optional":false,"name":"fullfillment.public.basic_types.Key"},"payload":{"i":2}}`,
				DebeziumVal: &deleteDebeziumVal,
			},
			{
				DebeziumKey: `{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"i"}],"optional":false,"name":"fullfillment.public.basic_types.Key"},"payload":{"i":2}}`,
				DebeziumVal: nil,
			},
		},
	}

	return []debeziumcommon.ChangeItemCanon{
		changeItemCanonInsert,
		changeItemCanonUpdate1,
		changeItemCanonUpdate2,
		changeItemCanonUpdate3,
		changeItemCanonDelete,
	}
}

func TestPg(t *testing.T) {
	InitCanon(t)
	testSuite := getArrChangeItemCanon()
	testSuite = testutil.FixTestSuite(t, testSuite, "fullfillment", "pguser", "pg")
	for _, testCase := range testSuite {
		testutil.CheckCanonizedDebeziumEvent(t, testCase.ChangeItem, "fullfillment", "pguser", "pg", false, testCase.DebeziumEvents)
	}
}
