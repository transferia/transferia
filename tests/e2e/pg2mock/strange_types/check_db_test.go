package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	"github.com/transferria/transferria/tests/helpers"
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

	sinker := &helpers.MockSink{}
	target := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinker },
		Cleanup:       model.DisabledCleanup,
	}
	transfer := helpers.MakeTransfer("fake", Source, &target, abstract.TransferTypeSnapshotOnly)
	checksTriggered := 0

	sinker.PushCallback = func(input []abstract.ChangeItem) {
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
	}

	_ = helpers.Activate(t, transfer)
	require.Equal(t, 1, checksTriggered)
}
