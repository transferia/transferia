package main

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var Source = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"))

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
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

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Snapshot", Snapshot)
	})
}

func Snapshot(t *testing.T) {
	sinker := &mockSinker{}
	transfer := transferhelpers.MakeTransfer(
		transferhelpers.TransferID,
		&Source,
		&model.MockDestination{SinkerFactory: func() abstract.Sinker {
			return sinker
		}},
		abstract.TransferTypeSnapshotOnly,
	)

	inputs := make(chan []abstract.ChangeItem, 100)
	sinker.pushCallback = func(input []abstract.ChangeItem) {
		time.Sleep(6 * time.Second)
		inputs <- input
	}

	_ = delivery.Activate(t, transfer)
}
