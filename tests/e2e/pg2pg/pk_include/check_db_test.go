package pk_include

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix(""))
	Target       = *pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
)

func init() {
	_ = os.Setenv("YC", "1")
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType)
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	Source.PreSteps.Constraint = true
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly)

	_ = helpers.Activate(t, transfer)

	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))

	sink, err := provider_postgres.NewSink(logger.Log, helpers.TransferID, Target.ToSinkParams(), helpers.EmptyRegistry())
	require.NoError(t, err)

	arrColSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "place_id", DataType: "int32", PrimaryKey: true, OriginalType: "pg:integer"},
		{ColumnName: "problem", DataType: "utf8", PrimaryKey: true, OriginalType: "pg:text"},
		{ColumnName: "date", DataType: "date", PrimaryKey: true, OriginalType: "pg:date"},
		{ColumnName: "numerator", DataType: "int32", PrimaryKey: false, OriginalType: "pg:integer"},
		{ColumnName: "denominator", DataType: "int32", PrimaryKey: false, OriginalType: "pg:integer"},
	})
	builder := helpers.NewChangeItemsBuilder("public", "problems_by_day", arrColSchema)

	require.NoError(t, sink.Push(builder.Inserts(t, []map[string]interface{}{
		{"place_id": 1, "problem": "pothole", "date": "2024-01-01", "numerator": 99, "denominator": 200},
	})))

	require.NoError(t, sink.Push(builder.Inserts(t, []map[string]interface{}{
		{"place_id": 3, "problem": "new_problem", "date": "2024-01-03", "numerator": 1, "denominator": 10},
	})))

	helpers.CheckRowsCount(t, Target, "public", "problems_by_day", 4)
}
