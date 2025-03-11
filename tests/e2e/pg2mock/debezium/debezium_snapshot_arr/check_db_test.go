package main

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/library/go/test/yatest"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	debeziumcommon "github.com/transferia/transferia/pkg/debezium/common"
	"github.com/transferia/transferia/pkg/debezium/testutil"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

var Source = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("init_source"))

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

//---------------------------------------------------------------------------------------------------------------------

func TestSnapshot(t *testing.T) {
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: Source.Port},
	))

	canonizedDebeziumKeyArr, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/tests/e2e/pg2mock/debezium/debezium_snapshot_arr/testdata/change_item_key.txt"))
	require.NoError(t, err)
	canonizedDebeziumValArr, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/tests/e2e/pg2mock/debezium/debezium_snapshot_arr/testdata/change_item_val.txt"))
	require.NoError(t, err)
	canonizedDebeziumVal := string(canonizedDebeziumValArr)

	//------------------------------------------------------------------------------

	sinker := &helpers.MockSink{}
	target := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinker },
		Cleanup:       model.DisabledCleanup,
	}
	transfer := helpers.MakeTransfer("fake", &Source, &target, abstract.TransferTypeSnapshotOnly)

	var changeItems []abstract.ChangeItem
	sinker.PushCallback = func(input []abstract.ChangeItem) {
		changeItems = append(changeItems, input...)
	}

	helpers.Activate(t, transfer)

	require.Equal(t, 5, len(changeItems))
	require.Equal(t, changeItems[0].Kind, abstract.InitShardedTableLoad)
	require.Equal(t, changeItems[1].Kind, abstract.InitTableLoad)
	require.Equal(t, changeItems[2].Kind, abstract.InsertKind)
	require.Equal(t, changeItems[3].Kind, abstract.DoneTableLoad)
	require.Equal(t, changeItems[4].Kind, abstract.DoneShardedTableLoad)

	fmt.Printf("changeItem dump: %s\n", changeItems[2].ToJSONString())
	canonizeTypes(t, &changeItems[2])

	testutil.CheckCanonizedDebeziumEvent(t, &changeItems[2], "fullfillment", "pguser", "pg", true, []debeziumcommon.KeyValue{{DebeziumKey: string(canonizedDebeziumKeyArr), DebeziumVal: &canonizedDebeziumVal}})

	changeItemBuf, err := json.Marshal(changeItems[2])
	require.NoError(t, err)
	changeItemDeserialized := helpers.UnmarshalChangeItem(t, changeItemBuf)
	testutil.CheckCanonizedDebeziumEvent(t, changeItemDeserialized, "fullfillment", "pguser", "pg", true, []debeziumcommon.KeyValue{{DebeziumKey: string(canonizedDebeziumKeyArr), DebeziumVal: &canonizedDebeziumVal}})
}

func canonizeTypes(t *testing.T, item *abstract.ChangeItem) {
	colNameToOriginalType := make(map[string]string)
	for _, el := range item.TableSchema.Columns() {
		colNameToOriginalType[el.ColumnName] = el.OriginalType
	}
	for i := range item.ColumnNames {
		currColName := item.ColumnNames[i]
		currColVal := item.ColumnValues[i]
		currOriginalType, ok := colNameToOriginalType[currColName]
		require.True(t, ok)
		fieldType := fmt.Sprintf("%T", currColVal)
		colNameToOriginalType[currColName] = fmt.Sprintf(`%s:%s`, currOriginalType, fieldType)
		if fieldType == "[]interface {}" && len(currColVal.([]interface{})) != 0 {
			currType2 := fmt.Sprintf(`%s:%s`, currOriginalType, fmt.Sprintf("%T", currColVal.([]interface{})[0]))
			colNameToOriginalType["    ELEM:"+currColName] = currType2
		}
	}
	canon.SaveJSON(t, colNameToOriginalType)
}
