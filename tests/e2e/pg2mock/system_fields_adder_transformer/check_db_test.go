package main

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	transferType = abstract.TransferTypeSnapshotAndIncrement
	source       = pgrecipe.RecipeSource()
	target       *model.MockDestination

	transformedTable    = *abstract.NewTableID("public", "test")
	notTransformedTable = *abstract.NewTableID("public", "test_not_transformed")
	targetItems         = make(map[abstract.TableID][]abstract.ChangeItem)
	waitTimeout         = 300 * time.Second
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	source.WithDefaults()
}

func TestSnapshotAndReplication(t *testing.T) {
	mu := sync.Mutex{}
	pushCallback := func(items []abstract.ChangeItem) error {
		mu.Lock()
		defer mu.Unlock()
		for _, item := range items {
			if slices.Contains([]abstract.Kind{abstract.InsertKind, abstract.UpdateKind}, item.Kind) {
				table := item.TableID()
				targetItems[table] = append(targetItems[table], item)
			}
		}
		return nil
	}
	target = &model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return &helpers.MockSink{PushCallback: pushCallback} },
		Cleanup:       model.Drop,
	}

	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: source.Port},
	))

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, transferType)

	require.NoError(t, transfer.TransformationFromJSON(`
{
  "transformers": [
    {
      "systemFieldsAdder": {
        "Fields": [
          { "FieldType": "id", "ColumnName": "__dt_id" },
          { "FieldType": "lsn", "ColumnName": "__dt_lsn" },
          { "FieldType": "tx_position", "ColumnName": "__dt_tx_position" },
          { "FieldType": "commit_time", "ColumnName": "__dt_commit_time" },
          { "FieldType": "tx_id", "ColumnName": "__dt_tx_id" }
        ],
        "Tables": {
          "includeTables": [ "^public.test$" ]
        }
      }
    }
  ]
}`))

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	t.Run("Snapshot", Snapshot)

	t.Run("Replication", Replication)

	t.Run("Validate", Validate)
}

func validateItem(t *testing.T, item abstract.ChangeItem, isTransformed bool) {
	if !isTransformed {
		require.Len(t, item.ColumnNames, 2, "ColumnNames")
		require.Len(t, item.ColumnValues, 2, "ColumnValues")
		require.Len(t, item.TableSchema.Columns(), 2, "TableSchema.Columns")
		return
	}
	require.Len(t, item.ColumnNames, 7, "ColumnNames")
	require.Len(t, item.ColumnValues, 7, "ColumnValues")
	require.Len(t, item.TableSchema.Columns(), 7, "TableSchema.Columns")
	asMap := item.AsMap()
	require.Equal(t, item.ID, asMap["__dt_id"])
	require.Equal(t, item.LSN, asMap["__dt_lsn"])
	require.Equal(t, item.Counter, asMap["__dt_tx_position"])
	require.Equal(t, item.CommitTime, asMap["__dt_commit_time"])
	require.Equal(t, item.TxID, asMap["__dt_tx_id"])
}

func Validate(t *testing.T) {
	for _, item := range targetItems[transformedTable] {
		validateItem(t, item, true)
	}
	for _, item := range targetItems[notTransformedTable] {
		validateItem(t, item, false)
	}
}

func Snapshot(t *testing.T) {
	n := 3
	require.NoError(t, helpers.WaitCond(waitTimeout, func() bool {
		logger.Log.Infof("For table %s got %d of %d items", transformedTable, len(targetItems[transformedTable]), n)
		return len(targetItems[transformedTable]) == n
	}))
	require.NoError(t, helpers.WaitCond(waitTimeout, func() bool {
		logger.Log.Infof("For table %s got %d of %d items", notTransformedTable, len(targetItems[notTransformedTable]), n)
		return len(targetItems[notTransformedTable]) == n
	}))
}

func Replication(t *testing.T) {
	replicationQuery := fmt.Sprintf(`
		INSERT INTO %[1]s VALUES (11, '11');         INSERT INTO %[2]s VALUES (11, '11');
		INSERT INTO %[1]s VALUES (100, '100');       INSERT INTO %[2]s VALUES (100, '100');
		UPDATE %[1]s SET val = '110' WHERE i = 100;  UPDATE %[2]s SET val = '110' WHERE i = 100;
	`, transformedTable, notTransformedTable)

	srcConn, err := postgres.MakeConnPoolFromSrc(source, logger.Log)
	require.NoError(t, err)
	_, err = srcConn.Exec(context.Background(), replicationQuery)
	srcConn.Close()
	require.NoError(t, err)

	n := 6
	require.NoError(t, helpers.WaitCond(waitTimeout, func() bool {
		logger.Log.Infof("For table %s got %d of %d items", transformedTable, len(targetItems[transformedTable]), n)
		return len(targetItems[transformedTable]) == n
	}))
	require.NoError(t, helpers.WaitCond(waitTimeout, func() bool {
		logger.Log.Infof("For table %s got %d of %d items", notTransformedTable, len(targetItems[notTransformedTable]), n)
		return len(targetItems[notTransformedTable]) == n
	}))
}
