package main

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/serde"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	transformerhelpers "github.com/transferia/transferia/tests/helpers/transformer"
	"github.com/transferia/transferia/tests/helpers/ydb"
	"github.com/transferia/transferia/tests/helpers/ydb/testdata"
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

func makeYdb2YdbFixPathUdf() transformerhelpers.SimpleTransformerApplyUDF {
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
	src := &provider_ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           testenv.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           testenv.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             []string{path, pathCompoundKey},
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		UseFullPaths:       true,
		ServiceAccountID:   "",
		ChangeFeedMode:     provider_ydb.ChangeFeedModeNewImage,
	}

	Target := &provider_ydb.YdbDestination{
		Database: src.Database,
		Token:    src.Token,
		Instance: src.Instance,
	}
	Target.WithDefaults()
	sinker, err := provider_ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	currChangeItem := testdata.YDBInitChangeItem(path)
	require.NoError(t, sinker.Push([]abstract.ChangeItem{*currChangeItem}))

	currCompoundChangeItem := testdata.YDBInitChangeItem(pathCompoundKey)
	currCompoundChangeItem = testdata.YDBStmtInsertValuesMultikey(
		t, pathCompoundKey, currCompoundChangeItem.ColumnValues,
		currCompoundChangeItem.ColumnValues[0],
		currCompoundChangeItem.ColumnValues[1],
	)
	require.NoError(t, sinker.Push([]abstract.ChangeItem{*currCompoundChangeItem}))

	dst := &provider_ydb.YdbDestination{
		Token:    model.SecretString(os.Getenv("YDB_TOKEN")),
		Database: testenv.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance: testenv.GetEnvOfFail(t, "YDB_ENDPOINT"),
	}
	transferhelpers.InitSrcDst("fake", src, dst, abstract.TransferTypeSnapshotAndIncrement)
	transfer := transferhelpers.MakeTransfer("fake", src, dst, abstract.TransferTypeSnapshotAndIncrement)

	fixPathTransformer := transformerhelpers.NewSimpleTransformer(t, makeYdb2YdbFixPathUdf(), serde.AnyTablesUdf)
	transformerhelpers.AddTransformer(t, transfer, fixPathTransformer)

	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	// inserts

	require.NoError(t, sinker.Push([]abstract.ChangeItem{
		*testdata.YDBStmtInsertValues(t, path, testdata.YDBTestValues1, 2),
		*testdata.YDBStmtInsertNulls(t, path, 3),
		*testdata.YDBStmtInsertValues(t, path, testdata.YDBTestValues3, 4),
		*testdata.YDBStmtInsertValuesMultikey(t, pathCompoundKey, testdata.YDBTestMultikeyValues1, 1, false),
		*testdata.YDBStmtInsertValuesMultikey(t, pathCompoundKey, testdata.YDBTestMultikeyValues2, 2, false),
		*testdata.YDBStmtInsertValuesMultikey(t, pathCompoundKey, testdata.YDBTestMultikeyValues3, 2, true),
	}))
	require.NoError(t, storage.WaitDestinationEqualRowsCount("", pathOut, storagecomparison.GetSampleableStorageByModel(t, dst), 60*time.Second, 4))
	require.NoError(t, storage.WaitDestinationEqualRowsCount("", pathCompoundKeyOut, storagecomparison.GetSampleableStorageByModel(t, dst), 60*time.Second, 4))

	// deletes

	require.NoError(t, sinker.Push([]abstract.ChangeItem{
		*testdata.YDBStmtDelete(t, path, 4),
	}))
	require.NoError(t, storage.WaitDestinationEqualRowsCount("", pathOut, storagecomparison.GetSampleableStorageByModel(t, dst), 60*time.Second, 3))

	require.NoError(t, sinker.Push([]abstract.ChangeItem{
		*testdata.YDBStmtDeleteCompoundKey(t, pathCompoundKey, 2, false),
	}))
	require.NoError(t, storage.WaitDestinationEqualRowsCount("", pathCompoundKeyOut, storagecomparison.GetSampleableStorageByModel(t, dst), 60*time.Second, 3))

	require.Equal(t, abstract.DeleteKind, extractedUpdatesAndDeletes[len(extractedUpdatesAndDeletes)-1].Kind)

	// canonize
	for testName, tablePath := range map[string]string{"simple table": pathOut, "compound key": pathCompoundKeyOut} {
		t.Run(testName, func(t *testing.T) {
			dump := ydb.PullDataFromTable(t,
				os.Getenv("YDB_TOKEN"),
				testenv.GetEnvOfFail(t, "YDB_DATABASE"),
				testenv.GetEnvOfFail(t, "YDB_ENDPOINT"),
				tablePath)
			for i := 0; i < len(dump); i++ {
				dump[i].CommitTime = 0
				dump[i].PartID = ""
			}
			canon.SaveJSON(t, dump)
		})
	}
}
