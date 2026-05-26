package snapshot_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	yt_recipe "github.com/transferia/transferia/pkg/providers/yt/recipe"
	yt_sink "github.com/transferia/transferia/pkg/providers/yt/sink"
	yt_storage "github.com/transferia/transferia/pkg/providers/yt/storage"
	"github.com/transferia/transferia/tests/helpers"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
)

var (
	TestTableName = "test_table"

	TestDstSchema = abstract.NewTableSchema(abstract.TableColumns{
		abstract.ColSchema{ColumnName: "author_id", DataType: string(ytschema.TypeString)},
		abstract.ColSchema{ColumnName: "id", DataType: string(ytschema.TypeString), PrimaryKey: true},
		abstract.ColSchema{ColumnName: "is_deleted", DataType: string(ytschema.TypeBoolean)},
	})

	TestSrcSchema = abstract.NewTableSchema(abstract.TableColumns{
		abstract.ColSchema{ColumnName: "author", DataType: string(ytschema.TypeString)}, // update
		abstract.ColSchema{ColumnName: "author_id", DataType: string(ytschema.TypeString)},
		abstract.ColSchema{ColumnName: "id", DataType: string(ytschema.TypeString), PrimaryKey: true},
		abstract.ColSchema{ColumnName: "is_deleted", DataType: string(ytschema.TypeBoolean)},
	})

	Dst = provider_yt.NewYtDestinationV1(provider_yt.YtDestination{
		Path:                     "//home/cdc/test/mock2yt_e2e",
		Cluster:                  os.Getenv("YT_PROXY"),
		CellBundle:               "default",
		PrimaryMedium:            "default",
		UseStaticTableOnSnapshot: false,
		Cleanup:                  model.DisabledCleanup,
	})
)

func TestYTSnapshotWithShuffledColumns(t *testing.T) {
	targetPort, err := helpers.GetPortFromStr(Dst.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "YT DST", Port: targetPort}))
	}()

	ytEnv, cancel := yt_recipe.NewEnv(t)
	defer cancel()

	ok, err := ytEnv.YT.NodeExists(context.Background(), ypath.Path(fmt.Sprintf("%s/%s", Dst.Path(), TestTableName)), nil)
	require.NoError(t, err)
	require.False(t, ok)

	Dst.WithDefaults()

	prepareDst(t)
	fillDestination(t)
	checkData(t)
}

func prepareDst(t *testing.T) {
	currentSink, err := yt_sink.NewSinker(Dst, helpers.TransferID, logger.Log, helpers.EmptyRegistry(), nil)
	require.NoError(t, err)

	require.NoError(t, currentSink.Push([]abstract.ChangeItem{{
		Kind:         abstract.InsertKind,
		Schema:       "",
		Table:        TestTableName,
		ColumnNames:  []string{"id", "author_id", "is_deleted"},
		ColumnValues: []interface{}{"000", "0", true},
		TableSchema:  TestDstSchema,
	}}))
}

func fillDestination(t *testing.T) {
	currentSink, err := yt_sink.NewSinker(Dst, helpers.TransferID, logger.Log, helpers.EmptyRegistry(), nil)
	require.NoError(t, err)
	defer require.NoError(t, currentSink.Close())

	require.NoError(t, currentSink.Push([]abstract.ChangeItem{
		{
			Kind:         abstract.InsertKind,
			Schema:       "",
			Table:        TestTableName,
			ColumnNames:  []string{"id", "author_id"},
			ColumnValues: []interface{}{"001", "1"},
			TableSchema:  TestDstSchema,
		},
	}))
	require.NoError(t, currentSink.Push([]abstract.ChangeItem{
		{
			Kind:         abstract.InsertKind,
			Schema:       "",
			Table:        TestTableName,
			ColumnNames:  []string{"id", "author", "author_id"},
			ColumnValues: []interface{}{"002", "test_author_2", "2"},
			TableSchema:  TestSrcSchema,
		},
	}))
}

func checkData(t *testing.T) {
	ytStorageParams := provider_yt.YtStorageParams{
		Token:   Dst.Token(),
		Cluster: os.Getenv("YT_PROXY"),
		Path:    Dst.Path(),
		Spec:    nil,
	}
	st, err := yt_storage.NewStorage(&ytStorageParams)
	require.NoError(t, err)

	td := abstract.TableDescription{
		Name:   TestTableName,
		Schema: "",
	}
	changeItems := helpers.LoadTable(t, st, td)

	var data []map[string]interface{}
	for _, row := range changeItems {
		data = append(data, row.AsMap())
	}

	require.Equal(t, data, []map[string]interface{}{
		{
			"author":     nil,
			"author_id":  "0",
			"id":         "000",
			"is_deleted": true,
		},
		{
			"author":     nil,
			"author_id":  "1",
			"id":         "001",
			"is_deleted": nil,
		},
		{
			"author":     "test_author_2",
			"author_id":  "2",
			"id":         "002",
			"is_deleted": nil,
		},
	})
}
