package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	"github.com/transferria/transferria/pkg/worker/tasks"
	"github.com/transferria/transferria/tests/helpers"
)

var Source = pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("init_source"))

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
	Source.SlotByteLagLimit = 100
}

//---------------------------------------------------------------------------------------------------------------------
// mockSinker

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

func TestSnapshot(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
		))
	}()

	sinker := new(mockSinker)
	transfer := helpers.MakeTransfer(
		helpers.TransferID,
		Source,
		&model.MockDestination{SinkerFactory: func() abstract.Sinker {
			return sinker
		}},
		abstract.TransferTypeSnapshotAndIncrement,
	)

	inputs := make(chan []abstract.ChangeItem, 100)
	sinker.pushCallback = func(input []abstract.ChangeItem) {
		time.Sleep(6 * time.Second)
		inputs <- input
	}

	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	err = snapshotLoader.UploadTables(context.Background(), tables.ConvertToTableDescriptions(), true)
	require.Error(t, err)
	require.True(t, abstract.IsFatal(err))
	require.Contains(t, err.Error(), "lag for replication slot")
}
