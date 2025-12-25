package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
	"go.ytsaurus.tech/yt/go/schema"
)

var (
	Source = pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("init_source"))
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

//---------------------------------------------------------------------------------------------------------------------

func TestSnapshot(t *testing.T) {
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: Source.Port},
	))

	//------------------------------------------------------------------------------

	sinker := mocksink.NewMockSink(nil)
	target := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinker },
		Cleanup:       model.DisabledCleanup,
	}
	transfer := helpers.MakeTransfer("fake", Source, &target, abstract.TransferTypeSnapshotOnly)
	checksTriggered := 0

	sinker.PushCallback = func(input []abstract.ChangeItem) error {
		for _, changeItem := range input {
			tableSchema := helpers.MakeTableSchema(&changeItem)
			fmt.Printf("changeItem=%s\n", changeItem.ToJSONString())

			//------------------------------------------------------------------------------
			// public.udt

			if changeItem.Kind == abstract.InsertKind && changeItem.Table == "udt" {
				checksTriggered++
				require.Equal(t, "RUB", changeItem.AsMap()["mycurrency"])
				require.Equal(t, schema.TypeString.String(), tableSchema.NameToTableSchema(t, "mycurrency").DataType)
				require.Equal(t, "pg:currency", tableSchema.NameToTableSchema(t, "mycurrency").OriginalType)
			}
		}
		return nil
	}

	_ = helpers.Activate(t, transfer)
	require.Equal(t, 1, checksTriggered)
}
