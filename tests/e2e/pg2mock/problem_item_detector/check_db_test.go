package problemitemdetector

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/transformer"
	problemitemdetector "github.com/transferia/transferia/pkg/transformer/registry/problem_item_detector"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	Source = pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("init_source"))
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

//---------------------------------------------------------------------------------------------------------------------

func TestSnapshotAndIncrement(t *testing.T) {
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: Source.Port},
	))

	//------------------------------------------------------------------------------

	sinker := &helpers.MockSink{}
	target := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinker },
		Cleanup:       model.DisabledCleanup,
	}

	transfer := helpers.MakeTransfer("fake", Source, &target, abstract.TransferTypeSnapshotOnly)
	transfer.Transformation = &model.Transformation{Transformers: &transformer.Transformers{
		DebugMode: false,
		Transformers: []transformer.Transformer{{
			problemitemdetector.TransformerType: problemitemdetector.Config{},
		}},
		ErrorsOutput: nil,
	}}

	sinker.PushCallback = func(input []abstract.ChangeItem) error {
		return xerrors.New("error")
	}

	cp := helpers.NewFakeCPErrRepl()
	worker, err := helpers.ActivateWithCP(transfer, cp, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "bad item detector found problem item")
	if worker != nil {
		defer worker.Close(t)
	}
}
