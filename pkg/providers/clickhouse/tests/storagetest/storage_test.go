//go:build !disable_clickhouse_provider

package storagetest

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	chrecipe "github.com/transferia/transferia/pkg/providers/clickhouse/recipe"
)

func TestShardedStorage(t *testing.T) {
	var (
		databaseName = "mtmobproxy"
		Shard1       = chrecipe.MustSource(
			chrecipe.WithPrefix("DBSHARD1_"),
			chrecipe.WithDatabase(databaseName),
			chrecipe.WithInitFile("dump/src_shard1.sql"),
		)
		Shard2 = chrecipe.MustSource(
			chrecipe.WithPrefix("DBSHARD2_"),
			chrecipe.WithDatabase(databaseName),
			chrecipe.WithInitFile("dump/src_shard2.sql"),
		)
		Shard3 = chrecipe.MustSource(
			chrecipe.WithPrefix("DBSHARD3_"),
			chrecipe.WithDatabase(databaseName),
			chrecipe.WithInitFile("dump/src_shard3.sql"),
		)
	)
	shard1, err := clickhouse.NewStorage(storageParams(t, Shard1), nil)
	require.NoError(t, err)
	shard2, err := clickhouse.NewStorage(storageParams(t, Shard2), nil)
	require.NoError(t, err)
	shard3, err := clickhouse.NewStorage(storageParams(t, Shard3), nil)
	require.NoError(t, err)
	shardedStorage := clickhouse.NewShardedStorage(map[string]*clickhouse.Storage{
		"shard1": shard1.(*clickhouse.Storage),
		"shard2": shard2.(*clickhouse.Storage),
		"shard3": shard3.(*clickhouse.Storage),
	})
	tables, err := shardedStorage.TableList(nil)
	require.NoError(t, err)
	tDescr := abstract.TableDescription{
		Name:   "sample_table",
		Schema: "mtmobproxy",
	}
	tID := tDescr.ID()
	_, ok := tables[tID]
	require.True(t, ok)
	tableSize, err := shardedStorage.TableSizeInBytes(tDescr.ID())
	require.NoError(t, err)
	logger.Log.Infof("table size: %v", tableSize)
	tableSize, err = shardedStorage.GetRowsCount(tID)
	require.NoError(t, err)
	logger.Log.Infof("rows count: %v", tableSize)
	var items []abstract.ChangeItem
	mutex := sync.Mutex{}
	require.NoError(t, shardedStorage.LoadTable(context.Background(), tDescr, func(input []abstract.ChangeItem) error {
		mutex.Lock()
		defer mutex.Unlock()
		items = append(items, input...)
		return nil
	}))
	require.Len(t, items, 15) // init table load 5 rows each shard
	require.Equal(t, abstract.InsertKind, items[0].Kind)
	require.Equal(t, abstract.InsertKind, items[len(items)-1].Kind) // no more init/done in storage
	abstract.Dump(items)
}

func storageParams(t *testing.T, source *model.ChSource) *model.ChStorageParams {
	params, err := source.ToStorageParams()
	require.NoError(t, err)

	return params
}
