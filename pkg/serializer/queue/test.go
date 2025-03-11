package queue

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
)

var arrMasterChangeItem []abstract.ChangeItem

func init() {
	masterJSON := `
	{
		"kind": "insert",
		"schema": "public",
		"table": "timmyb32r_test_tm_2174_pg_src2",
		"columnnames": ["id", "val1", "val2"],
		"columnvalues": [4, 5, 6],
		"table_schema": [
			{"path": "", "name": "id",   "type": "int32", "key": true,  "required": false},
			{"path": "", "name": "val1", "type": "int32", "key": false, "required": false},
			{"path": "", "name": "val2", "type": "int32", "key": false, "required": false}
		]
	}`
	masterChangeItem, _ := abstract.UnmarshalChangeItem([]byte(masterJSON))

	arrMasterChangeItem = make([]abstract.ChangeItem, 0)
	for i := 0; i < 5; i++ {
		arrMasterChangeItem = append(arrMasterChangeItem, *masterChangeItem)
	}
}

func checkTestCase(t *testing.T, serializerF serializerFactory, enabled bool, maxChangeItems int, maxMessageSize int64, expectedLen int) {
	batcher := model.Batching{
		Enabled:        enabled,
		Interval:       0,
		MaxChangeItems: maxChangeItems,
		MaxMessageSize: maxMessageSize,
	}
	currSerializer := serializerF(batcher)
	myMap, err := currSerializer.Serialize(arrMasterChangeItem)
	require.NoError(t, err)
	require.Len(t, myMap, 1)
	for _, arr := range myMap {
		require.Len(t, arr, expectedLen)
	}
}

type serializerFactory = func(batchingSettings model.Batching) Serializer
type masterChangeItemSize = func(in abstract.ChangeItem) int
type batchSize = func(elemSize, elemsNum int) int64

func commonTest(t *testing.T, serializerF serializerFactory, changeItemSizer masterChangeItemSize, batchSizer batchSize) {
	masterChangeItemLen := changeItemSizer(arrMasterChangeItem[0])

	t.Run("no batching", func(t *testing.T) {
		checkTestCase(t, serializerF, false, 0, 0, 5)
	})

	// #ChangeItems

	t.Run("batching by #changeItems #1", func(t *testing.T) {
		checkTestCase(t, serializerF, true, 1, 0, 5)
	})
	t.Run("batching by #changeItems #2", func(t *testing.T) {
		checkTestCase(t, serializerF, true, 2, 0, 3)
	})

	// MessageSize

	t.Run("batching by messageSize #1 (every message violates constraint)", func(t *testing.T) {
		checkTestCase(t, serializerF, true, 0, 1, 5)
	})
	t.Run("batching by messageSize #2 (every message fit exactly one batch)", func(t *testing.T) {
		checkTestCase(t, serializerF, true, 0, batchSizer(masterChangeItemLen, 1), 5)
	})
	t.Run("batching by messageSize #3 (2 messages fit exactly one batch minus 1)", func(t *testing.T) {
		checkTestCase(t, serializerF, true, 0, batchSizer(masterChangeItemLen, 2)-1, 5)
	})

	t.Run("batching by messageSize #4 (2 messages fit exactly one batch)", func(t *testing.T) {
		checkTestCase(t, serializerF, true, 0, batchSizer(masterChangeItemLen, 2), 3)
	})
	t.Run("batching by messageSize #5 (2 messages fit exactly one batch plus 1)", func(t *testing.T) {
		checkTestCase(t, serializerF, true, 0, batchSizer(masterChangeItemLen, 2)+1, 3)
	})

	t.Run("batching by messageSize #6 (3 messages fit exactly one batch minus 1)", func(t *testing.T) {
		checkTestCase(t, serializerF, true, 0, batchSizer(masterChangeItemLen, 3)-1, 3)
	})
	t.Run("batching by messageSize #7 (3 messages fit exactly one batch)", func(t *testing.T) {
		checkTestCase(t, serializerF, true, 0, batchSizer(masterChangeItemLen, 3), 2)
	})
	t.Run("batching by messageSize #8 (3 messages fit exactly one batch plus 1)", func(t *testing.T) {
		checkTestCase(t, serializerF, true, 0, batchSizer(masterChangeItemLen, 3)+1, 2)
	})

	// combo

	t.Run("batching by both: #changeItems & messageSize (1 changeItem allowed, 2 messages fit exactly one batch)", func(t *testing.T) {
		checkTestCase(t, serializerF, true, 1, batchSizer(masterChangeItemLen, 2), 5)
	})
	t.Run("batching by both: #changeItems & messageSize (2 changeItems allowed, 2 messages fit exactly one batch)", func(t *testing.T) {
		checkTestCase(t, serializerF, true, 2, batchSizer(masterChangeItemLen, 2), 3)
	})
	t.Run("batching by both: #changeItems & messageSize (3 changeItems allowed, 2 messages fit exactly one batch)", func(t *testing.T) {
		checkTestCase(t, serializerF, true, 2, batchSizer(masterChangeItemLen, 2), 3)
	})
	t.Run("batching by both: #changeItems & messageSize (3 changeItems allowed, 1 messages fit exactly one batch)", func(t *testing.T) {
		checkTestCase(t, serializerF, true, 2, batchSizer(masterChangeItemLen, 1), 5)
	})
}
