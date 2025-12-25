package storage

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	yt_provider "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/recipe"
	ytsink "github.com/transferia/transferia/pkg/providers/yt/sink"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

func emptyRegistry() metrics.Registry {
	return solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()})
}

func buildDynamicSchema(schema []yt_provider.ColumnSchema) []map[string]string {
	res := make([]map[string]string, len(schema))
	for idx, col := range schema {
		res[idx] = map[string]string{
			"name": col.Name,
			"type": string(col.YTType),
		}

		if col.Primary {
			res[idx]["sort_order"] = "ascending"
		}
	}

	return res
}

func TestYtStorage_TableList(t *testing.T) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()

	ctx := context.Background()

	_, err := env.YT.CreateNode(ctx, ypath.Path("//home/cdc/test/yt_storage_test"), yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	defer func() {
		err := env.YT.RemoveNode(ctx, ypath.Path("//home/cdc/test/yt_storage_test"), &yt.RemoveNodeOptions{Recursive: true})
		require.NoError(t, err)
	}()
	require.NoError(t, err)

	_, err = env.YT.CreateNode(ctx, ypath.Path("//home/cdc/test/yt_storage_test/__test"), yt.NodeTable, &yt.CreateNodeOptions{
		Attributes: map[string]interface{}{
			"schema": buildDynamicSchema([]yt_provider.ColumnSchema{
				{
					Name:    "Column_1",
					YTType:  "int8",
					Primary: true,
				},
				{
					Name:    "Column_2",
					YTType:  "int8",
					Primary: false,
				},
			},
			),
			"dynamic":            true,
			"tablet_cell_bundle": "default",
		},
	})
	require.NoError(t, err)

	Target := yt_provider.NewYtDestinationV1(yt_provider.YtDestination{
		Path:          "//home/cdc/test/yt_storage_test",
		CellBundle:    "default",
		PrimaryMedium: "default",
		Atomicity:     yt.AtomicityFull,
		Cluster:       os.Getenv("YT_PROXY"),
	})

	Target.WithDefaults()

	sinker, err := ytsink.NewSinker(Target, "", logger.Log, emptyRegistry(), coordinator.NewFakeClient(), nil)
	require.NoError(t, err)

	err = sinker.Push([]abstract.ChangeItem{
		{
			ID:         242571256,
			CommitTime: 1601382119000000000,
			Kind:       abstract.InsertKind,
			Table:      "__test",
			TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{
					ColumnName: "Column_1",
					DataType:   "int8",
					PrimaryKey: true,
				},
				{
					ColumnName: "Column_2",
					DataType:   "int8",
					PrimaryKey: false,
				},
			}),
			ColumnNames: []string{
				"Column_1",
				"Column_2",
			},
			ColumnValues: []interface{}{
				1,
				-123,
			},
		},
	})
	require.NoError(t, err)

	storageParams := yt_provider.YtStorageParams{
		Token:                 Target.Token(),
		Cluster:               os.Getenv("YT_PROXY"),
		Path:                  Target.Path(),
		Spec:                  Target.Spec().GetConfig(),
		DisableProxyDiscovery: Target.GetConnectionData().DisableProxyDiscovery,
	}

	st, err := NewStorage(&storageParams)
	require.NoError(t, err)

	tables, err := st.TableList(nil)
	require.NoError(t, err)
	for tID := range tables {
		logger.Log.Infof("input table: %v %v", tID.Namespace, tID.Name)
	}
	require.Equal(t, 1, len(tables))

	tableDescriptions := tables.ConvertToTableDescriptions()
	upCtx := util.ContextWithTimestamp(context.Background(), time.Now())
	err = st.LoadTable(upCtx, tableDescriptions[0], func(input []abstract.ChangeItem) error {
		abstract.Dump(input)
		return nil
	})
	require.NoError(t, err)

	size, err := st.TableSizeInBytes(
		abstract.TableID{
			Name: "__test",
		},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(0), size)

	err = st.LoadTopBottomSample(tableDescriptions[0], func(input []abstract.ChangeItem) error {
		abstract.Dump(input)
		return nil
	})
	require.NoError(t, err)

	err = st.LoadRandomSample(tableDescriptions[0], func(input []abstract.ChangeItem) error {
		abstract.Dump(input)
		return nil
	})
	require.NoError(t, err)
}
