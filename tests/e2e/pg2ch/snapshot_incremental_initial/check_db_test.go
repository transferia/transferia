package snapshot

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse/chrecipe"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	databaseName = "public"
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump/pg"))
	Target       = *chrecipe.MustTarget(chrecipe.WithInitDir("dump/ch"), chrecipe.WithDatabase(databaseName))
)

func init() {
	_ = os.Setenv("YC", "1")                                              // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func testSnapshot(t *testing.T, source *provider_postgres.PgSource, target clickhouse_model.ChDestination, incremental abstract.IncrementalTable, expectedRows uint64) {
	t.Helper()

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: source.Port},
			helpers.LabeledPort{Label: "CH target Native", Port: target.NativePort},
			helpers.LabeledPort{Label: "CH target HTTP", Port: target.HTTPPort},
		))
	}()

	source.DBTables = []string{incremental.Namespace + "." + incremental.Name}
	source.SlotID = ""
	transfer := helpers.MakeTransferForIncrementalSnapshot(
		helpers.TransferID+"_"+incremental.Name,
		source,
		&target,
		TransferType,
		incremental.Namespace,
		incremental.Name,
		incremental.CursorField,
		incremental.InitialState,
		0,
	)
	_ = helpers.Activate(t, transfer)

	destination := helpers.GetSampleableStorageByModel(t, target)
	defer destination.Close()
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		incremental.Namespace,
		incremental.Name,
		destination,
		time.Minute,
		expectedRows,
	))
}

func TestSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name         string
		incremental  abstract.IncrementalTable
		expectedRows uint64
	}{
		{
			name: "quoted timestamp",
			incremental: abstract.IncrementalTable{
				Namespace:    "public",
				Name:         "__test_incremental",
				CursorField:  "updated_at",
				InitialState: `'2022-09-27 00:00:00Z'`,
			},
			expectedRows: 1000,
		},
		{
			name: "to_date for timestamptz",
			incremental: abstract.IncrementalTable{
				Namespace:    "public",
				Name:         "__test_incremental_timestamptz",
				CursorField:  "updated_at",
				InitialState: `to_date('2023-01-01', 'YYYY-MM-DD')`,
			},
			expectedRows: 2,
		},
		{
			name: "timestamp literal",
			incremental: abstract.IncrementalTable{
				Namespace:    "public",
				Name:         "__test_incremental_timestamp_literal",
				CursorField:  "update_time",
				InitialState: `timestamp'2000-03-16'`,
			},
			expectedRows: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := *Source
			testSnapshot(t, &source, Target, tc.incremental, tc.expectedRows)
		})
	}
}
