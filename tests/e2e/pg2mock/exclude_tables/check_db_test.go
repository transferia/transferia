package excludetables

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	Source = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"))
	Target = model.MockDestination{
		SinkerFactory: makeMockSinker,
	}
)

func makeMockSinker() abstract.Sinker {
	return &mockSinker{}
}

type mockSinker struct {
	pushCallback func([]abstract.ChangeItem)
}

func (s *mockSinker) Close() error {
	return nil
}

func (s *mockSinker) Push(input []abstract.ChangeItem) error {
	s.pushCallback(input)
	return nil
}

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
	Target.WithDefaults()
}

func checkItem(t *testing.T, item abstract.ChangeItem, key int, value string) {
	require.EqualValues(t, len(item.ColumnValues), 2)
	require.EqualValues(t, key, item.ColumnValues[0])
	require.EqualValues(t, value, item.ColumnValues[1])
}

type tableName = string

func TestExcludeTablesWithEmptyWhitelist(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
		))
	}()

	sinker := &mockSinker{}
	source := Source
	source.DBTables = []string{}
	source.ExcludedTables = []string{"public.second_table"}
	transfer := model.Transfer{
		ID:  "test_id",
		Src: &source,
		Dst: &model.MockDestination{SinkerFactory: func() abstract.Sinker {
			return sinker
		}},
	}

	tableEvents := map[tableName][]abstract.ChangeItem{}
	sinker.pushCallback = func(input []abstract.ChangeItem) {
		for _, item := range input {
			slice, ok := tableEvents[item.Table]
			if !ok {
				slice = make([]abstract.ChangeItem, 0, 1)
			}
			slice = append(slice, item)
			tableEvents[item.Table] = slice
		}
	}

	ctx := context.Background()
	srcConn, err := postgres.MakeConnPoolFromSrc(&source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	_, err = srcConn.Exec(ctx, `SELECT pg_create_logical_replication_slot('testslot', 'wal2json')`)
	require.NoError(t, err)
	defer srcConn.Exec(ctx, `SELECT pg_drop_replication_slot('testslot')`) //nolint

	r, err := srcConn.Exec(ctx, `INSERT INTO first_table VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')`)
	require.NoError(t, err)
	require.EqualValues(t, 5, r.RowsAffected())
	defer srcConn.Exec(ctx, `DELETE FROM first_table`) //nolint

	r, err = srcConn.Exec(ctx, `INSERT INTO second_table VALUES (11, 'aa'), (22, 'bb'), (33, 'cc'), (44, 'dd'), (55, 'ee')`)
	require.NoError(t, err)
	require.EqualValues(t, 5, r.RowsAffected())
	defer srcConn.Exec(ctx, `DELETE FROM second_table`) //nolint

	worker := local.NewLocalWorker(coordinator.NewFakeClient(), &transfer, helpers.EmptyRegistry(), logger.Log)
	worker.Start()
	time.Sleep(10 * time.Second)
	_ = worker.Stop()

	delete(tableEvents, "__consumer_keeper")
	require.EqualValues(t, 1, len(tableEvents))
	slice, ok := tableEvents["first_table"]
	require.True(t, ok)
	require.EqualValues(t, 5, len(slice))
	checkItem(t, slice[0], 1, "a")
	checkItem(t, slice[1], 2, "b")
	checkItem(t, slice[2], 3, "c")
	checkItem(t, slice[3], 4, "d")
	checkItem(t, slice[4], 5, "e")
}

func TestExcludeTablesWithNonEmptyWhitelist(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
		))
	}()

	sinker := &mockSinker{}
	source := Source
	source.DBTables = []string{"public.\".first_table\"", "public.\"second_table\""}
	source.ExcludedTables = []string{"public.\"first_table\""}
	transfer := model.Transfer{
		ID:  "test_id",
		Src: &source,
		Dst: &model.MockDestination{SinkerFactory: func() abstract.Sinker {
			return sinker
		}},
	}

	tableEvents := map[tableName][]abstract.ChangeItem{}
	sinker.pushCallback = func(input []abstract.ChangeItem) {
		for _, item := range input {
			slice, ok := tableEvents[item.Table]
			if !ok {
				slice = make([]abstract.ChangeItem, 0, 1)
			}
			slice = append(slice, item)
			tableEvents[item.Table] = slice
		}
	}

	ctx := context.Background()
	srcConn, err := postgres.MakeConnPoolFromSrc(&source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	_, err = srcConn.Exec(ctx, `SELECT pg_create_logical_replication_slot('testslot', 'wal2json')`)
	require.NoError(t, err)
	defer srcConn.Exec(ctx, `SELECT pg_drop_replication_slot('testslot')`) //nolint

	r, err := srcConn.Exec(ctx, `INSERT INTO first_table VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')`)
	require.NoError(t, err)
	require.EqualValues(t, 5, r.RowsAffected())
	defer srcConn.Exec(ctx, `DELETE FROM first_table`) //nolint

	r, err = srcConn.Exec(ctx, `INSERT INTO second_table VALUES (11, 'aa'), (22, 'bb'), (33, 'cc'), (44, 'dd'), (55, 'ee')`)
	require.NoError(t, err)
	require.EqualValues(t, 5, r.RowsAffected())
	defer srcConn.Exec(ctx, `DELETE FROM second_table`) //nolint

	worker := local.NewLocalWorker(coordinator.NewFakeClient(), &transfer, helpers.EmptyRegistry(), logger.Log)
	worker.Start()
	time.Sleep(10 * time.Second)
	_ = worker.Stop()

	delete(tableEvents, "__consumer_keeper")
	require.EqualValues(t, 1, len(tableEvents))
	slice, ok := tableEvents["second_table"]
	require.True(t, ok)
	require.EqualValues(t, 5, len(slice))
	checkItem(t, slice[0], 11, "aa")
	checkItem(t, slice[1], 22, "bb")
	checkItem(t, slice[2], 33, "cc")
	checkItem(t, slice[3], 44, "dd")
	checkItem(t, slice[4], 55, "ee")
}
