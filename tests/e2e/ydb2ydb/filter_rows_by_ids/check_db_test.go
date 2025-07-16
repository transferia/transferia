package filterrowsbyids

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	filterrowsbyids "github.com/transferia/transferia/pkg/transformer/registry/filter_rows_by_ids"
	"github.com/transferia/transferia/tests/helpers"
	"github.com/transferia/transferia/tests/helpers/serde"
	"go.ytsaurus.tech/yt/go/schema"
)

var path = "dectest/test-src"
var pathOut = "dectest/test-dst"

var tableMapping = map[string]string{
	path: pathOut,
}

func makeYdb2YdbFixPathUdf() helpers.SimpleTransformerApplyUDF {
	return func(t *testing.T, items []abstract.ChangeItem) abstract.TransformerResult {
		newChangeItems := make([]abstract.ChangeItem, 0)
		for i := range items {
			items[i].Table = tableMapping[items[i].Table]
			newChangeItems = append(newChangeItems, items[i])
		}
		return abstract.TransformerResult{
			Transformed: newChangeItems,
			Errors:      nil,
		}
	}
}

func ydbInsertChangeItem(tablePath string, values []interface{}) abstract.ChangeItem {
	return abstract.ChangeItem{
		ID:         0,
		LSN:        0,
		CommitTime: 0,
		Kind:       abstract.InsertKind,
		Schema:     "",
		Table:      tablePath,
		TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
			{PrimaryKey: true, Required: false, ColumnName: "id", DataType: "uint64", OriginalType: "ydb:Uint64"},
			{PrimaryKey: false, Required: true, ColumnName: "id2", DataType: string(schema.TypeBytes), OriginalType: "ydb:String"},
			{PrimaryKey: false, Required: false, ColumnName: "id3", DataType: string(schema.TypeString), OriginalType: "ydb:Utf8"},
			{PrimaryKey: false, Required: false, ColumnName: "value", DataType: string(schema.TypeInt32), OriginalType: "ydb:Int32"},
		}),
		ColumnNames:  []string{"id", "id2", "id3", "value"},
		ColumnValues: values,
	}
}

func ydbUpdateChangeItem(tablePath string, values []interface{}) abstract.ChangeItem {
	item := ydbInsertChangeItem(tablePath, values)
	item.Kind = abstract.UpdateKind
	return item
}

func TestSnapshotAndReplication(t *testing.T) {
	src := &ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             []string{path},
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

	currChangeItem := ydbInsertChangeItem(path, []interface{}{1, []byte("ID0_suffix"), "ID2_0", 1})
	require.NoError(t, sinker.Push([]abstract.ChangeItem{currChangeItem}))

	dst := &ydb.YdbDestination{
		Token:    model.SecretString(os.Getenv("YDB_TOKEN")),
		Database: helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance: helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
	}
	helpers.InitSrcDst("fake", src, dst, abstract.TransferTypeSnapshotAndIncrement)
	transfer := helpers.MakeTransfer("fake", src, dst, abstract.TransferTypeSnapshotAndIncrement)

	fixPathTransformer := helpers.NewSimpleTransformer(t, makeYdb2YdbFixPathUdf(), serde.AnyTablesUdf)
	helpers.AddTransformer(t, transfer, fixPathTransformer)

	transformer, err := filterrowsbyids.NewFilterRowsByIDsTransformer(
		filterrowsbyids.Config{
			Tables: filter.Tables{
				IncludeTables: []string{},
			},
			Columns: filter.Columns{
				IncludeColumns: []string{"id2", "id3"},
			},
			AllowedIDs: []string{
				"ID1",
				"ID2_2",
			},
		},
		logger.Log,
	)
	require.NoError(t, err)
	helpers.AddTransformer(t, transfer, transformer)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	// inserts
	require.NoError(t, sinker.Push([]abstract.ChangeItem{
		ydbInsertChangeItem(path, []interface{}{1, []byte("ID0_suffix"), "ID2_0", 1}),
		ydbInsertChangeItem(path, []interface{}{2, []byte("ID1_suffix"), "ID2_1", 2}),
		ydbInsertChangeItem(path, []interface{}{3, []byte("ID2_suffix"), "ID2_2", 3}),
		ydbInsertChangeItem(path, []interface{}{4, []byte("ID3_suffix"), "ID2_3", 4}),
	}))
	require.NoError(t, helpers.WaitDestinationEqualRowsCount("", pathOut, helpers.GetSampleableStorageByModel(t, dst), 60*time.Second, 2))

	// updates
	require.NoError(t, sinker.Push([]abstract.ChangeItem{
		ydbInsertChangeItem(path, []interface{}{1, []byte("ID0_suffix"), "ID2_0", 2}),
		ydbInsertChangeItem(path, []interface{}{2, []byte("ID1_suffix"), "ID2_1", 3}),
		ydbInsertChangeItem(path, []interface{}{3, []byte("ID2_suffix"), "ID2_2", 4}),
		ydbInsertChangeItem(path, []interface{}{4, []byte("ID3_suffix"), "ID2_3", 5}),
	}))
	require.NoError(t, helpers.WaitDestinationEqualRowsCount("", pathOut, helpers.GetSampleableStorageByModel(t, dst), 60*time.Second, 2))

	// canonize
	for testName, tablePath := range map[string]string{"simple table": pathOut} {
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
