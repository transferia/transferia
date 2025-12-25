package cdcpartialactivate

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	chrecipe "github.com/transferia/transferia/pkg/providers/clickhouse/recipe"
	pgrecipe "github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	Source = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("dump"), pgrecipe.WithDBTables("public.measurement_declarative"))
	Target = *chrecipe.MustTarget(chrecipe.WithInitDir("dump/ch"), chrecipe.WithDatabase("public"))
)

const CursorField = "id"
const CursorValue = "5"

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "CH target Native", Port: Target.NativePort},
			helpers.LabeledPort{Label: "CH target HTTP", Port: Target.HTTPPort},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Load", Load)
	})
}

func Load(t *testing.T) {
	Source.CollapseInheritTables = true
	transfer := helpers.MakeTransferForIncrementalSnapshot(
		helpers.TransferID,
		&Source,
		&Target,
		abstract.TransferTypeSnapshotOnly,
		"public",
		"measurement_declarative",
		CursorField,
		CursorValue,
		1,
	)
	transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{"public.measurement_declarative"}}
	_ = helpers.Activate(t, transfer)
	helpers.CheckRowsCount(t, Target, "", "measurement_declarative", 5)
}
