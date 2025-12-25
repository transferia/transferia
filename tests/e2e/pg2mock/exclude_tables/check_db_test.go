package excludetables

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	Source = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"))
	Target = model.MockDestination{
		SinkerFactory: makeMockSinker,
	}
	TransferType = abstract.TransferTypeIncrementOnly
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
	Target.WithDefaults()
}

//---------------------------------------------------------------------------------------------------------------------
// mockSinker

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

//---------------------------------------------------------------------------------------------------------------------

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
	source := &Source
	source.DBTables = []string{}
	source.ExcludedTables = []string{"public.second_table"}

	dst := &model.MockDestination{SinkerFactory: func() abstract.Sinker {
		return sinker
	}}

	trasferID := helpers.GenerateTransferID("TestExcludeTablesWithEmptyWhitelist")
	helpers.InitSrcDst(trasferID, source, dst, TransferType)
	transfer := &model.Transfer{
		ID:   "test_id",
		Src:  source,
		Dst:  dst,
		Type: TransferType,
	}

	tableEvents := map[tableName][]abstract.ChangeItem{}
	counter := 0
	sinker.pushCallback = func(input []abstract.ChangeItem) {
		for _, item := range input {
			counter++
			slice, ok := tableEvents[item.Table]
			if !ok {
				slice = make([]abstract.ChangeItem, 0, 1)
			}
			slice = append(slice, item)
			tableEvents[item.Table] = slice
		}
	}

	// activate

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	ctx := context.Background()
	srcConn, err := postgres.MakeConnPoolFromSrc(source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	r, err := srcConn.Exec(ctx, `INSERT INTO first_table VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')`)
	require.NoError(t, err)
	require.EqualValues(t, 5, r.RowsAffected())

	r, err = srcConn.Exec(ctx, `INSERT INTO second_table VALUES (11, 'aa'), (22, 'bb'), (33, 'cc'), (44, 'dd'), (55, 'ee')`)
	require.NoError(t, err)
	require.EqualValues(t, 5, r.RowsAffected())

	// wait
	for {
		if counter == 5 {
			break
		}
		time.Sleep(1 * time.Second)
	}

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
