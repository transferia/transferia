package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

type stopCondition func(t *testing.T, tableName string, src provider_postgres.PgSource, dst provider_postgres.PgDestination) error

func untilStoragesEqual(t *testing.T, tableName string, src provider_postgres.PgSource, dst provider_postgres.PgDestination) error {
	params := storagecomparison.NewCompareStorageParams().WithTableFilter(makeTableFilter(tableName))
	return storagecomparison.WaitStoragesSynced(t, src, dst, 15, params)
}

func untilDestinationRowCountEquals(rowCount uint64) stopCondition {
	return func(t *testing.T, tableName string, src provider_postgres.PgSource, dst provider_postgres.PgDestination) error {
		return storage.WaitDestinationEqualRowsCount("public", tableName, storagecomparison.GetSampleableStorageByModel(t, dst), time.Minute, rowCount)
	}
}

func untilTimeElapsesAndStoragesEqual(delay time.Duration, expectedDstRowCount uint64) stopCondition {
	return func(t *testing.T, tableName string, src provider_postgres.PgSource, dst provider_postgres.PgDestination) error {
		time.Sleep(delay)
		params := storagecomparison.NewCompareStorageParams().WithTableFilter(makeTableFilter(tableName))
		if err := storagecomparison.WaitStoragesSynced(t, src, dst, 15, params); err != nil {
			return err
		}
		return storage.WaitDestinationEqualRowsCount("public", tableName, storagecomparison.GetSampleableStorageByModel(t, dst), 5*time.Second, expectedDstRowCount)
	}
}

func testReplicationWorks(t *testing.T, slotID, tableName string, perTransactionPush bool, waitStopCondition stopCondition) {
	source := *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"), pgrecipe.WithDBTables(fmt.Sprintf("public.%s", tableName)))
	source.SlotID = slotID
	target := *pgrecipe.RecipeTarget(pgrecipe.WithInitDir("init_target"))
	target.PerTransactionPush = perTransactionPush

	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: source.Port},
			network.LabeledPort{Label: "PG target", Port: target.Port},
		))
	}()

	TransferType := abstract.TransferTypeIncrementOnly
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &source, &target, TransferType)

	replicationWorker := local.NewLocalWorker(
		coordinator.NewFakeClient(),
		transferhelpers.MakeTransfer(transferhelpers.TransferID, &source, &target, TransferType),
		testmetrics.EmptyRegistry(),
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
		for _, table := range storagecomparison.FilterTechnicalTables(tables) {
			if table.Name != tableName {
				continue
			}
			filteredTables = append(filteredTables, table)
		}
		return filteredTables
	}
}
