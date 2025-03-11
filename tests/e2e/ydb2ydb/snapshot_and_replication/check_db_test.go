package main

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/metrics/solomon"
	"github.com/transferria/transferria/library/go/test/canon"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers/ydb"
	"github.com/transferria/transferria/tests/helpers"
	"github.com/transferria/transferria/tests/helpers/serde"
)

var path = "dectest/test-src"
var pathOut = "dectest/test-dst"
var pathCompoundKey = "dectest/test-src-compound"
var pathCompoundKeyOut = "dectest/test-dst-compound"

var tableMapping = map[string]string{
	path:            pathOut,
	pathCompoundKey: pathCompoundKeyOut,
}

var extractedUpdatesAndDeletes []abstract.ChangeItem
var extractedInserts []abstract.ChangeItem

func makeYdb2YdbFixPathUdf() helpers.SimpleTransformerApplyUDF {
	return func(t *testing.T, items []abstract.ChangeItem) abstract.TransformerResult {
		newChangeItems := make([]abstract.ChangeItem, 0)
		for i := range items {
			items[i].Table = tableMapping[items[i].Table]

			row, _ := json.Marshal(items[i])
			fmt.Printf("changeItem:%s\n", string(row))
			newChangeItems = append(newChangeItems, items[i])

			currItem := items[i]
			if currItem.Kind == abstract.InsertKind {
				require.NotZero(t, len(currItem.KeyCols()))
				extractedInserts = append(extractedInserts, currItem)
			} else if currItem.Kind == abstract.UpdateKind || currItem.Kind == abstract.DeleteKind {
				require.NotZero(t, len(currItem.KeyCols()))
				extractedUpdatesAndDeletes = append(extractedUpdatesAndDeletes, currItem)
			}

			for j := range currItem.ColumnNames {
				if currItem.ColumnNames[j] == "String_" {
					if currItem.ColumnValues[j] == nil {
						continue
					}
					require.Equal(t, fmt.Sprintf("%T", []byte{}), fmt.Sprintf("%T", currItem.ColumnValues[j]))
				}
			}
		}
		return abstract.TransformerResult{
			Transformed: newChangeItems,
			Errors:      nil,
		}
	}
}

func TestSnapshotAndReplication(t *testing.T) {
	src := &ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             []string{path, pathCompoundKey},
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		UseFullPaths:       true,
		ServiceAccountID:   "",
		ChangeFeedMode:     ydb.ChangeFeedModeNewImage,
	}

	Target := &ydb.YdbDestination{
		Database: src.Database,
		Token:    src.Token,
		Instance: src.Instance,
	}
	Target.WithDefaults()
	sinker, err := ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	currChangeItem := helpers.YDBInitChangeItem(path)
	require.NoError(t, sinker.Push([]abstract.ChangeItem{*currChangeItem}))

	currCompoundChangeItem := helpers.YDBInitChangeItem(pathCompoundKey)
	currCompoundChangeItem = helpers.YDBStmtInsertValuesMultikey(
		t, pathCompoundKey, currCompoundChangeItem.ColumnValues,
		currCompoundChangeItem.ColumnValues[0],
		currCompoundChangeItem.ColumnValues[1],
	)
	require.NoError(t, sinker.Push([]abstract.ChangeItem{*currCompoundChangeItem}))

	dst := &ydb.YdbDestination{
		Token:    model.SecretString(os.Getenv("YDB_TOKEN")),
		Database: helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance: helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
	}
	helpers.InitSrcDst("fake", src, dst, abstract.TransferTypeSnapshotAndIncrement)
	transfer := helpers.MakeTransfer("fake", src, dst, abstract.TransferTypeSnapshotAndIncrement)

	fixPathTransformer := helpers.NewSimpleTransformer(t, makeYdb2YdbFixPathUdf(), serde.AnyTablesUdf)
	helpers.AddTransformer(t, transfer, fixPathTransformer)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	// inserts

	require.NoError(t, sinker.Push([]abstract.ChangeItem{
		*helpers.YDBStmtInsertValues(t, path, helpers.YDBTestValues1, 2),
		*helpers.YDBStmtInsertNulls(t, path, 3),
		*helpers.YDBStmtInsertValues(t, path, helpers.YDBTestValues3, 4),
		*helpers.YDBStmtInsertValuesMultikey(t, pathCompoundKey, helpers.YDBTestMultikeyValues1, 1, false),
		*helpers.YDBStmtInsertValuesMultikey(t, pathCompoundKey, helpers.YDBTestMultikeyValues2, 2, false),
		*helpers.YDBStmtInsertValuesMultikey(t, pathCompoundKey, helpers.YDBTestMultikeyValues3, 2, true),
	}))
	require.NoError(t, helpers.WaitDestinationEqualRowsCount("", pathOut, helpers.GetSampleableStorageByModel(t, dst), 60*time.Second, 4))
	require.NoError(t, helpers.WaitDestinationEqualRowsCount("", pathCompoundKeyOut, helpers.GetSampleableStorageByModel(t, dst), 60*time.Second, 4))

	// deletes

	require.NoError(t, sinker.Push([]abstract.ChangeItem{
		*helpers.YDBStmtDelete(t, path, 4),
	}))
	require.NoError(t, helpers.WaitDestinationEqualRowsCount("", pathOut, helpers.GetSampleableStorageByModel(t, dst), 60*time.Second, 3))

	require.NoError(t, sinker.Push([]abstract.ChangeItem{
		*helpers.YDBStmtDeleteCompoundKey(t, pathCompoundKey, 2, false),
	}))
	require.NoError(t, helpers.WaitDestinationEqualRowsCount("", pathCompoundKeyOut, helpers.GetSampleableStorageByModel(t, dst), 60*time.Second, 3))

	require.Equal(t, abstract.DeleteKind, extractedUpdatesAndDeletes[len(extractedUpdatesAndDeletes)-1].Kind)

	// canonize
	for testName, tablePath := range map[string]string{"simple table": pathOut, "compound key": pathCompoundKeyOut} {
		t.Run(testName, func(t *testing.T) {
			dump := helpers.YDBPullDataFromTable(t,
				os.Getenv("YDB_TOKEN"),
				helpers.GetEnvOfFail(t, "YDB_DATABASE"),
				helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
				tablePath)
			for i := 0; i < len(dump); i++ {
				dump[i].CommitTime = 0
				dump[i].PartID = ""
			}
			canon.SaveJSON(t, dump)
		})
	}
}
