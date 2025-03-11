package main

import (
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
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement

	Source = pgrecipe.RecipeSource(
		pgrecipe.WithPrefix(""),
		pgrecipe.WithInitDir("dump"),
		pgrecipe.WithEdit(func(pg *postgres.PgSource) {
			pg.CollapseInheritTables = true
			pg.UseFakePrimaryKey = true
		}))
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

func TestSnapshotAndIncrement(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
		))
	}()

	// Dst
	sinker := &helpers.MockSink{}
	target := &model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinker },
		Cleanup:       model.Drop,
	}

	var result []abstract.ChangeItem
	sinker.PushCallback = func(input []abstract.ChangeItem) {
		for _, i := range input {
			if i.Table == "__consumer_keeper" {
				continue
			}
			if !i.IsRowEvent() {
				continue
			}
			require.Equal(t, "\"public\".\"actions\"", i.TableID().String())
			result = append(result, i)
		}
	}

	transfer := helpers.MakeTransfer(helpers.TransferID, Source, target, TransferType)
	transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{"public.actions"}}
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)
	require.Equal(t, 8, len(result))

	// replication
	sinkToSource, err := postgres.NewSink(logger.Log, helpers.TransferID, Source.ToSinkParams(), helpers.EmptyRegistry())
	require.NoError(t, err)

	schema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "added_at", DataType: ytschema.TypeDate.String(), PrimaryKey: false},
		{ColumnName: "external_id", DataType: ytschema.TypeInt32.String(), PrimaryKey: false},
		{ColumnName: "tenant", DataType: ytschema.TypeInt32.String(), PrimaryKey: false},
	})
	valuesToInsert := []map[string]interface{}{
		{"added_at": "2024-03-07", "external_id": 1, "tenant": 2},
		{"added_at": "2024-01-04", "external_id": 1, "tenant": 1},
		{"added_at": "2024-02-08", "external_id": 2, "tenant": 1},
	}

	builder := helpers.NewChangeItemsBuilder("public", "actions", schema)
	require.NoError(t, sinkToSource.Push(builder.Inserts(t, valuesToInsert)))

	//-----------------------------------------------------------------------------------------------------------------

	helpers.CheckRowsCount(t, transfer.Src, "public", "actions", 11)
	helpers.CheckRowsCount(t, transfer.Src, "public", "actions_2023", 3)
	helpers.CheckRowsCount(t, transfer.Src, "public", "actions_2024_01", 3)
	helpers.CheckRowsCount(t, transfer.Src, "public", "actions_2024_01_01", 3)
	helpers.CheckRowsCount(t, transfer.Src, "public", "actions_2024_01_02", 0)
	helpers.CheckRowsCount(t, transfer.Src, "public", "actions_2024_02", 3)
	helpers.CheckRowsCount(t, transfer.Src, "public", "actions_2024_02_01", 2)
	helpers.CheckRowsCount(t, transfer.Src, "public", "actions_2024_02_02", 1)
	helpers.CheckRowsCount(t, transfer.Src, "public", "actions_2024_03", 2)
	helpers.CheckRowsCount(t, transfer.Src, "public", "actions_2024_03_01", 0)
	helpers.CheckRowsCount(t, transfer.Src, "public", "actions_2024_03_02", 2)

	for {
		if len(result) == 11 {
			break
		}
		time.Sleep(time.Second)
	}
}
