package snapshot

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/database_operations"
)

// A returned database replica applies a previous EXCHANGE when its DDL queue
// catches up. Reads before that barrier still describe the previous live table.
type registrationDDLQueueOperations struct {
	database_operations.DatabaseOperations
	catchUp func()
}

func (o *registrationDDLQueueOperations) SyncDatabaseDDLOnNode(ctx context.Context, _ string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	o.catchUp()
	return nil
}

func TestTableRegistrationReadsUUIDAfterPreviousDatabaseDDL(test *testing.T) {
	ctx := context.Background()
	const operationID = "ddl-registration-operation"
	sink, queries := newRegistrationSink(test, newRegistrationState(), operationID)
	test.Cleanup(func() { StopStagingDDLKeepalives(sink.transferID, operationID) })
	require.NoError(test, sink.initDatabaseOperations(ctx))

	previousUUID := uuid.NewString()
	publishedUUID := uuid.NewString()
	queries.ExpectQuery("SELECT toUInt8").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
	queries.ExpectQuery("SELECT version").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("26.3.1"))
	queries.ExpectQuery("SELECT engine FROM system.databases").WithArgs("test").
		WillReturnRows(sqlmock.NewRows([]string{"engine"}).AddRow("Replicated"))
	uuidQuery := queries.ExpectQuery("SELECT toString\\(uuid\\)").WithArgs("test", "events").
		WillReturnRows(sqlmock.NewRows([]string{"uuid"}).AddRow(previousUUID))
	queries.ExpectQuery("SELECT count\\(\\) FROM system.mutations").WithArgs("test", "events").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	queries.ExpectQuery("SELECT create_table_query, engine").WithArgs("test", "events").
		WillReturnRows(sqlmock.NewRows([]string{"ddl", "engine", "dependencies"}).
			AddRow("CREATE TABLE events (id UInt64, version UInt64) ENGINE = MergeTree() ORDER BY id", "MergeTree", uint64(0)))
	queuedOperations := &registrationDDLQueueOperations{
		DatabaseOperations: sink.databaseOperations,
		catchUp: func() {
			uuidQuery.WillReturnRows(sqlmock.NewRows([]string{"uuid"}).AddRow(publishedUUID))
		},
	}
	sink.databaseOperations = queuedOperations

	table, err := sink.prepareTableState(ctx, abstract.ChangeItem{
		Kind:        abstract.InitShardedTableLoad,
		Schema:      "public",
		Table:       "events",
		TableSchema: snapshotTestSchema,
	})
	require.NoError(test, err)
	// Staging and publication wait for the database queue anyway. Registration
	// must not retain a UUID which becomes obsolete at that later barrier.
	require.NoError(test, queuedOperations.SyncDatabaseDDLOnNode(ctx, "unused"))
	require.Equal(test, publishedUUID, table.Replicas()[0].PreviousTableUUID)
}
