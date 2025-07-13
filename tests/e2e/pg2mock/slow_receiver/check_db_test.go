package slowreceiver

import (
	"context"
	"fmt"
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

func TestSlowReceiver(t *testing.T) {
	testAtLeastOnePushHasMultipleItems(t)
}

func testAtLeastOnePushHasMultipleItems(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
		))
	}()

	sinker := &mockSinker{}
	target := &model.MockDestination{SinkerFactory: func() abstract.Sinker {
		return sinker
	}}
	helpers.InitSrcDst(helpers.TransferID, &Source, target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, target, TransferType)

	pushedInputs := 0
	inputs := make(chan []abstract.ChangeItem, 100)
	sinker.pushCallback = func(input []abstract.ChangeItem) {
		if pushedInputs >= 5 {
			// DEBUG
			fmt.Println("timmyb32rQQQ :: pushedInputs >= 5")
			// DEBUG
			return
		}

		time.Sleep(1 * time.Second)
		var inputCopy []abstract.ChangeItem
		for _, item := range input {
			if item.Table == "__test1" {
				inputCopy = append(inputCopy, item)
			}
		}
		if len(inputCopy) > 0 {
			inputs <- inputCopy
		}

		pushedInputs += len(inputCopy)
		if pushedInputs >= 5 {
			close(inputs)
		}
	}

	// activate

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	// insert 5 events

	ctx := context.Background()
	srcConn, err := postgres.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	r, err := srcConn.Exec(ctx, `INSERT INTO __test1 VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')`)
	require.NoError(t, err)
	require.EqualValues(t, 5, r.RowsAffected())

	// check

	var concat []abstract.ChangeItem
	var i int
	var maxLen int
	for input := range inputs {
		fmt.Printf("Input items %d: %v\n", i, input)
		require.Greater(t, len(input), 0)
		concat = append(concat, input...)
		if maxLen < len(input) {
			maxLen = len(input)
		}
		i++
	}
	require.Greater(t, maxLen, 1)
	require.EqualValues(t, 5, len(concat))
	for i, item := range concat {
		require.EqualValues(t, 2, len(item.ColumnValues))
		require.EqualValues(t, fmt.Sprintf("%d", i+1), fmt.Sprintf("%v", item.ColumnValues[0]))
		require.EqualValues(t, fmt.Sprintf("%c", 'a'+i), fmt.Sprintf("%v", item.ColumnValues[1]))
	}
}
