package snapshot

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"testing"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/sharding"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
	"go.ytsaurus.tech/library/go/core/log"
)

// Count calls at the remote-store boundary, including reads of absent records.
type registrationState struct {
	*serialState
	keyReads       int
	tableReads     int
	loseWriteReply bool
}

var _ coordinator.TransferState = (*registrationState)(nil)

func newRegistrationState() *registrationState {
	return &registrationState{
		serialState:    newSerialState(),
		keyReads:       0,
		tableReads:     0,
		loseWriteReply: false,
	}
}

func (s *registrationState) GetTransferStateKeys(transferID string) ([]string, error) {
	s.keyReads++
	return s.serialState.GetTransferStateKeys(transferID)
}

func (s *registrationState) GetTransferStateByKeys(transferID string, keys []string) (map[string]*coordinator.TransferStateData, error) {
	s.tableReads++
	return s.serialState.GetTransferStateByKeys(transferID, keys)
}

func (s *registrationState) SetTransferState(transferID string, values map[string]*coordinator.TransferStateData) error {
	if err := s.serialState.SetTransferState(transferID, values); err != nil {
		return err
	}
	if s.loseWriteReply {
		s.loseWriteReply = false
		return errors.New("lost registration reply")
	}
	return nil
}

type registrationHTTPClient struct {
	httpclient.HTTPClient
}

var _ httpclient.HTTPClient = (*registrationHTTPClient)(nil)

func (*registrationHTTPClient) QueryStream(_ context.Context, _ log.Logger, _ *conn_clickhouse.Host, _ any, params url.Values) (io.ReadCloser, error) {
	if params.Get("session_check") == "1" {
		return nil, &clickhouse_go.Exception{Code: 372, Message: "Session not found"}
	}
	return io.NopCloser(strings.NewReader("")), nil
}

func newRegistrationSink(test *testing.T, state *registrationState, operationID string) (*Sink, sqlmock.Sqlmock) {
	test.Helper()
	database, queries, err := sqlmock.New()
	require.NoError(test, err)
	queries.ExpectQuery("SELECT toUInt8").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
	queries.ExpectQuery("SELECT engine FROM system.databases").WithArgs("test").WillReturnRows(sqlmock.NewRows([]string{"engine"}).AddRow("Atomic"))
	sink, err := newTestSink(
		logger.Log,
		&clickhouse_model.ChDestination{Database: "test", Cleanup: model.Drop, Hosts: []string{"unused"}},
		state,
		"registration-transfer",
		operationID,
		[][]ch_eo_common.Node{{ch_eo_common.NewNode(&conn_clickhouse.Host{Name: "unused"}, database, &registrationHTTPClient{HTTPClient: nil})}},
		false,
		sharding.ConstSharder(),
		func(item abstract.ChangeItem) string { return item.Table },
		func(abstract.ChangeItem) (string, error) {
			return "CREATE TABLE t (id UInt64, version UInt64) ENGINE = MergeTree() ORDER BY id", nil
		},
		nil,
	)
	require.NoError(test, err)
	test.Cleanup(func() {
		sink.stopStagingDDLKeepalives()
		require.NoError(test, sink.uploader.Close())
		testUploadBySink.Delete(sink)
		testStorageBySink.Delete(sink)
		queries.ExpectClose()
		require.NoError(test, database.Close())
		require.NoError(test, queries.ExpectationsWereMet())
	})
	return sink, queries
}

func expectTableRegistration(queries sqlmock.Sqlmock, tableName string) {
	queries.ExpectQuery("SELECT toUInt8").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
	queries.ExpectQuery("SELECT version").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("26.3.1"))
	queries.ExpectQuery("SELECT engine FROM system.databases").WithArgs("test").WillReturnRows(sqlmock.NewRows([]string{"engine"}).AddRow("Atomic"))
	queries.ExpectQuery("SELECT toString\\(uuid\\)").WithArgs("test", tableName).WillReturnRows(sqlmock.NewRows([]string{"uuid"}))
	queries.ExpectQuery("SELECT toUInt8").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
	queries.ExpectQuery("SELECT getMaxTableNameLengthForDatabase").WithArgs("test").WillReturnRows(sqlmock.NewRows([]string{"limit"}).AddRow(255))
}

func registerTestTable(test *testing.T, sink *Sink, queries sqlmock.Sqlmock, tableIndex int) {
	test.Helper()
	tableName := fmt.Sprintf("table_%d", tableIndex)
	expectTableRegistration(queries, tableName)
	err := sink.Push([]abstract.ChangeItem{{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: tableName, TableSchema: snapshotTestSchema}})
	// This fixture only tests registration; stop idle keepalives before their
	// first probe so it never needs a ClickHouse server or a network request.
	sink.stopStagingDDLKeepalives()
	require.NoError(test, err)
}

func TestTableRegistrationCoordinatorReadsAreLinear(test *testing.T) {
	for _, tableCount := range []int{100, 500} {
		test.Run(fmt.Sprint(tableCount), func(test *testing.T) {
			state := newRegistrationState()
			sink, queries := newRegistrationSink(test, state, "operation")
			for tableIndex := 0; tableIndex < tableCount; tableIndex++ {
				registerTestTable(test, sink, queries, tableIndex)
			}
			require.Len(test, state.data, tableCount)
			require.Equal(test, 1, state.keyReads, "inventory must be read once per registration sink")
			require.LessOrEqual(test, state.tableReads, tableCount+1, "new tables must not reread all previously registered tables")
		})
	}
}

func TestTableRegistrationRejectsCollidingDestination(test *testing.T) {
	state := newRegistrationState()
	sink, queries := newRegistrationSink(test, state, "operation")
	registerTestTable(test, sink, queries, 0)

	err := sink.Push([]abstract.ChangeItem{{Kind: abstract.InitShardedTableLoad, Schema: "other", Table: "table_0", TableSchema: snapshotTestSchema}})
	require.ErrorContains(test, err, "map to the same destination table_0")
	require.Len(test, state.data, 1)
	require.Equal(test, 1, state.keyReads)
}

func TestTableRegistrationRestoresDestinationNamesOnce(test *testing.T) {
	const tableCount = 100
	state := newRegistrationState()
	first, firstQueries := newRegistrationSink(test, state, "operation")
	for tableIndex := 0; tableIndex < tableCount; tableIndex++ {
		registerTestTable(test, first, firstQueries, tableIndex)
	}
	state.keyReads, state.tableReads = 0, 0

	recovery, queries := newRegistrationSink(test, state, "operation")
	for tableIndex := tableCount; tableIndex < 2*tableCount; tableIndex++ {
		registerTestTable(test, recovery, queries, tableIndex)
	}
	require.Equal(test, 1, state.keyReads)
	require.LessOrEqual(test, state.tableReads, 2*tableCount+1, "recovery must load old destination names once")

	err := recovery.Push([]abstract.ChangeItem{{Kind: abstract.InitShardedTableLoad, Schema: "other", Table: "table_0", TableSchema: snapshotTestSchema}})
	require.ErrorContains(test, err, "map to the same destination table_0")
	require.Len(test, state.data, 2*tableCount)
	require.Equal(test, 1, state.keyReads)
}

func TestTableRegistrationRejectsPreviousOperation(test *testing.T) {
	state := newRegistrationState()
	previous, previousQueries := newRegistrationSink(test, state, "previous")
	registerTestTable(test, previous, previousQueries, 0)
	current, _ := newRegistrationSink(test, state, "current")

	err := current.Push([]abstract.ChangeItem{{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "table_1", TableSchema: snapshotTestSchema}})
	require.ErrorContains(test, err, "previous ClickHouse snapshot still requires cleanup")
	require.Nil(test, current.sourceTableByDestinationName)
	require.Len(test, state.data, 1)
}

func TestTableRegistrationLostReplyDoesNotUpdateDestinationNames(test *testing.T) {
	state := newRegistrationState()
	state.loseWriteReply = true
	sink, queries := newRegistrationSink(test, state, "operation")
	expectTableRegistration(queries, "table_0")
	err := sink.Push([]abstract.ChangeItem{{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "table_0", TableSchema: snapshotTestSchema}})
	sink.stopStagingDDLKeepalives()
	require.ErrorContains(test, err, "lost registration reply")
	require.NotContains(test, sink.sourceTableByDestinationName, "table_0")
	require.Len(test, state.data, 1, "the registration succeeded despite the lost reply")

	recovery, _ := newRegistrationSink(test, state, "operation")
	err = recovery.Push([]abstract.ChangeItem{{Kind: abstract.InitShardedTableLoad, Schema: "other", Table: "table_0", TableSchema: snapshotTestSchema}})
	require.ErrorContains(test, err, "map to the same destination table_0")
	require.Equal(test, abstract.TableID{Namespace: "public", Name: "table_0"}, recovery.sourceTableByDestinationName["table_0"])
}

func TestTableRegistrationAllowsTTLAndMacrosOutsideClauses(test *testing.T) {
	for _, tc := range []struct {
		name   string
		ddl    string
		engine string
	}{
		{
			name:   "column comment",
			ddl:    "CREATE TABLE t (id UInt64 COMMENT 'daily TTL report', version UInt64) ENGINE=MergeTree() ORDER BY id",
			engine: "MergeTree",
		},
		{
			name:   "default literal",
			ddl:    "CREATE TABLE t (id UInt64, version UInt64, label String DEFAULT '{table}:{database} TTL report') ENGINE=MergeTree() ORDER BY id",
			engine: "MergeTree",
		},
		{
			name:   "quoted identifier",
			ddl:    "CREATE TABLE t (id UInt64, version UInt64, `daily TTL report {table}` String) ENGINE=MergeTree() ORDER BY id",
			engine: "MergeTree",
		},
		{
			name:   "SQL comment",
			ddl:    "CREATE TABLE t (id UInt64, version UInt64) ENGINE=MergeTree() /* TTL {database} */ ORDER BY id",
			engine: "MergeTree",
		},
		{
			name:   "replica name",
			ddl:    "CREATE TABLE t (id UInt64, version UInt64) ENGINE=ReplicatedMergeTree('/stable/path', '{table}-{database}') ORDER BY id",
			engine: "ReplicatedMergeTree",
		},
	} {
		test.Run(tc.name, func(test *testing.T) {
			state := newRegistrationState()
			sink, queries := newRegistrationSink(test, state, "operation")
			queries.ExpectQuery("SELECT toUInt8").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
			queries.ExpectQuery("SELECT version").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("26.3.1"))
			queries.ExpectQuery("SELECT engine FROM system.databases").WithArgs("test").WillReturnRows(sqlmock.NewRows([]string{"engine"}).AddRow("Atomic"))
			queries.ExpectQuery("SELECT toString\\(uuid\\)").WithArgs("test", "t").WillReturnRows(sqlmock.NewRows([]string{"uuid"}).AddRow("old-uuid"))
			queries.ExpectQuery("SELECT count\\(\\) FROM system.mutations").WithArgs("test", "t").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
			queries.ExpectQuery("SELECT create_table_query").WithArgs("test", "t").WillReturnRows(sqlmock.NewRows([]string{"ddl", "engine", "dependencies"}).AddRow(tc.ddl, tc.engine, 0))
			if strings.HasPrefix(tc.engine, "Replicated") {
				queries.ExpectQuery("SELECT total_replicas").WithArgs("test", "t").WillReturnRows(sqlmock.NewRows([]string{"replicas", "path", "name"}).AddRow(1, "/stable/path", "replica"))
			}
			queries.ExpectQuery("SELECT toUInt8").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
			queries.ExpectQuery("SELECT getMaxTableNameLengthForDatabase").WithArgs("test").WillReturnRows(sqlmock.NewRows([]string{"limit"}).AddRow(255))

			err := sink.Push([]abstract.ChangeItem{{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t", TableSchema: snapshotTestSchema}})
			sink.stopStagingDDLKeepalives()
			require.NoError(test, err)
			require.Len(test, state.data, 1)
		})
	}
}
