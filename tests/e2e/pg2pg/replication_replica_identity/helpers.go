package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/tests/helpers"
)

type stopCondition func(t *testing.T, tableName string, src postgres.PgSource, dst postgres.PgDestination) error

func untilStoragesEqual(t *testing.T, tableName string, src postgres.PgSource, dst postgres.PgDestination) error {
	params := helpers.NewCompareStorageParams().WithTableFilter(makeTableFilter(tableName))
	return helpers.WaitStoragesSynced(t, src, dst, 15, params)
}

func untilDestinationRowCountEquals(rowCount uint64) stopCondition {
	return func(t *testing.T, tableName string, src postgres.PgSource, dst postgres.PgDestination) error {
		return helpers.WaitDestinationEqualRowsCount("public", tableName, helpers.GetSampleableStorageByModel(t, dst), time.Minute, rowCount)
	}
}

func untilTimeElapsesAndStoragesEqual(delay time.Duration, expectedDstRowCount uint64) stopCondition {
	return func(t *testing.T, tableName string, src postgres.PgSource, dst postgres.PgDestination) error {
		time.Sleep(delay)
		params := helpers.NewCompareStorageParams().WithTableFilter(makeTableFilter(tableName))
		if err := helpers.WaitStoragesSynced(t, src, dst, 15, params); err != nil {
			return err
		}
		return helpers.WaitDestinationEqualRowsCount("public", tableName, helpers.GetSampleableStorageByModel(t, dst), 5*time.Second, expectedDstRowCount)
	}
}

func testReplicationWorks(t *testing.T, slotID, tableName string, perTransactionPush bool, waitStopCondition stopCondition) {
	source := *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"), pgrecipe.WithDBTables(fmt.Sprintf("public.%s", tableName)))
	source.SlotID = slotID
	target := *pgrecipe.RecipeTarget(pgrecipe.WithInitDir("init_target"))
	target.PerTransactionPush = perTransactionPush

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: source.Port},
			helpers.LabeledPort{Label: "PG target", Port: target.Port},
		))
	}()

	TransferType := abstract.TransferTypeIncrementOnly
	helpers.InitSrcDst(helpers.TransferID, &source, &target, TransferType)

	replicationWorker := local.NewLocalWorker(
		coordinator.NewFakeClient(),
		helpers.MakeTransfer(helpers.TransferID, &source, &target, TransferType),
		helpers.EmptyRegistry(),
		logger.Log,
	)
	replicationWorker.Start()

	require.NoError(t, waitStopCondition(t, tableName, source, target))

	err := replicationWorker.Stop()
	require.NoError(t, err)
}

func makeTableFilter(tableName string) func(tables abstract.TableMap) []abstract.TableDescription {
	return func(tables abstract.TableMap) []abstract.TableDescription {
		var filteredTables []abstract.TableDescription
		for _, table := range helpers.FilterTechnicalTables(tables) {
			if table.Name != tableName {
				continue
			}
			filteredTables = append(filteredTables, table)
		}
		return filteredTables
	}
}
