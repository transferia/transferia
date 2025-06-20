//go:build !disable_postgres_provider

package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
)

func TestInheritTableStorageSize(t *testing.T) {
	src := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("test_scripts"))
	src.CollapseInheritTables = true
	storage, err := postgres.NewStorage(src.ToStorageParams(nil))
	require.NoError(t, err)
	err = storage.BeginPGSnapshot(context.TODO())
	require.NoError(t, err)
	logger.Log.Infof("create snapshot: %v, ts: %v", storage.ShardedStateLSN, storage.ShardedStateTS)
	tid := abstract.TableID{Name: "__test_parent", Namespace: "public"}
	size, err := storage.TableSizeInBytes(tid)
	require.NoError(t, err)
	require.Equal(t, 15319040, int(size))
}

func TestInheritTableSharding(t *testing.T) {
	src := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("test_scripts"))
	storage, err := postgres.NewStorage(src.ToStorageParams(nil))
	require.NoError(t, err)
	storage.SetLoadDescending(true)
	err = storage.BeginPGSnapshot(context.TODO())
	require.NoError(t, err)
	logger.Log.Infof("create snapshot: %v, ts: %v", storage.ShardedStateLSN, storage.ShardedStateTS)
	ctx := context.Background()
	tables, err := storage.ShardTable(ctx, abstract.TableDescription{
		Name:   "__test_parent",
		Schema: "public",
		Filter: "",
		EtaRow: 0,
		Offset: 0,
	})
	require.NoError(t, err)
	require.Len(t, tables, 2)
	var res []abstract.ChangeItem
	for _, tbl := range tables {
		require.NoError(t, storage.LoadTable(ctx, tbl, func(input []abstract.ChangeItem) error {
			for _, row := range input {
				if row.IsRowEvent() {
					res = append(res, row)
				}
			}
			return nil
		}))
	}
	require.Equal(t, 2*100_000-1, len(res))
}
