package database_operations

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"testing"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	conn "github.com/transferia/transferia/pkg/providers/clickhouse/conn"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
	"github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/transfer_state"
	"go.ytsaurus.tech/library/go/core/log"
)

const sessionNotFoundCode = 372

type databaseFixture struct {
	tablesClient       *ch_eo_common.Client
	dstConfig          *clickhouse_model.ChDestination
	logger             log.Logger
	transferID         string
	databaseOperations DatabaseOperations
	store              *transfer_state.State
	table              *transfer_state.TableState
	storage            coordinator.TransferState
}

func newDatabaseFixture(test *testing.T) *databaseFixture {
	test.Helper()
	cfg := &clickhouse_model.ChDestination{Database: "test", Cleanup: model.Drop}
	client := ch_eo_common.NewClient(cfg, logger.Log, [][]ch_eo_common.Node{{ch_eo_common.NewNode(&conn_clickhouse.Host{Name: "unused"}, new(sql.DB), newSessionHTTPClient(func(url.Values) error { return nil }))}})
	storage := newSerialState()
	store := transfer_state.NewState(storage, "transfer", "operation")
	table := store.NewTable(abstract.TableID{Name: "events"}, "events", "", false)
	operations := newAtomic(logger.Log, "transfer", cfg.Database, cfg.Cleanup, client, table.Snapshot(), testCheckpoint(table))
	operations.requireAllReplicas = true
	return &databaseFixture{tablesClient: client, dstConfig: cfg, logger: logger.Log, transferID: "transfer", databaseOperations: operations, store: store, table: table, storage: storage}
}
func configureTestNodes(fixture *databaseFixture, configure func([][]ch_eo_common.Node) [][]ch_eo_common.Node) {
	fixture.tablesClient = ch_eo_common.NewClient(fixture.dstConfig, fixture.logger, configure(fixture.tablesClient.NodesByShard()))
	if fixture.databaseOperations == nil {
		return
	}
	if _, replicated := fixture.databaseOperations.(*replicatedOperations); replicated {
		fixture.databaseOperations = newReplicated(fixture.logger, fixture.transferID, fixture.dstConfig.Database, fixture.dstConfig.Cleanup, fixture.tablesClient, fixture.table.Snapshot(), testCheckpoint(fixture.table))
	} else {
		operations := newAtomic(fixture.logger, fixture.transferID, fixture.dstConfig.Database, fixture.dstConfig.Cleanup, fixture.tablesClient, fixture.table.Snapshot(), testCheckpoint(fixture.table))
		operations.requireAllReplicas = fixture.databaseOperations.(*atomicOperations).requireAllReplicas
		fixture.databaseOperations = operations
	}
}
func commandsForTest(fixture *databaseFixture) *tableCommands {
	if operations, ok := fixture.databaseOperations.(*atomicOperations); ok {
		return operations.tableCommands
	}
	return fixture.databaseOperations.(*replicatedOperations).tableCommands
}
func (f *databaseFixture) initDatabaseOperations(ctx context.Context) error {
	var err error
	f.databaseOperations, err = New(ctx, f.logger, f.transferID, f.dstConfig.Database, f.dstConfig.Cleanup, f.tablesClient)
	return err
}
func (f *databaseFixture) buildCandidateTable(ctx context.Context, table *transfer_state.TableState, parts []ch_eo_common.ReadyPart) error {
	candidates, err := f.databaseOperations.ForTable(table.Snapshot(), testCheckpoint(table)).BuildCandidates(ctx, parts)
	if err != nil {
		return err
	}
	return table.RecordCandidates(ctx, candidates)
}

type sessionHTTPClient struct {
	httpclient.HTTPClient
	query func(url.Values) error
}

func newSessionHTTPClient(query func(url.Values) error) *sessionHTTPClient {
	return &sessionHTTPClient{HTTPClient: nil, query: query}
}

func (c *sessionHTTPClient) QueryStream(_ context.Context, _ log.Logger, _ *conn_clickhouse.Host, query any, params url.Values) (io.ReadCloser, error) {
	if queryText, ok := query.(string); ok && queryText == "SELECT 1 FORMAT Null" && params.Get("session_id") == "" && params.Get("query_id") == "" {
		return io.NopCloser(strings.NewReader("")), nil
	}
	err := c.query(params)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(strings.NewReader("")), nil
}

func newDatabaseReadFixture(test *testing.T) (*databaseFixture, sqlmock.Sqlmock) {
	test.Helper()
	database, mock, err := sqlmock.New()
	require.NoError(test, err)
	// A leaked result would prevent the next attempt from acquiring a connection.
	database.SetMaxOpenConns(1)
	test.Cleanup(
		func() {
			require.NoError(test, mock.ExpectationsWereMet())
			_ = database.Close()
		},
	)
	sink := newDatabaseFixture(test)
	configureTestNodes(sink, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		nodes[0][0].DB = database

		return nodes
	},
	)
	return sink, mock
}

func TestCommandUsesLegacyCompatibleParameters(test *testing.T) {
	sink := newDatabaseFixture(test)
	lostReply := io.EOF
	calls := 0
	configureTestNodes(sink, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		nodes[0][0].HTTP = newSessionHTTPClient(
			func(params url.Values) error {
				calls++
				if calls == 1 {
					require.Equal(test, "session", params.Get("query_id"))
					require.Equal(test, "1", params.Get("session_check"))
					require.Equal(test, "2", params.Get("replication_alter_partitions_sync"))
					require.Empty(test, params.Get("close_session"))
					return lostReply
				}
				return &clickhouse_go.Exception{Code: sessionNotFoundCode, Message: "Session not found"}
			},
		)

		return nodes
	},
	)
	command := ch_eo_common.NewCommand("session", "ALTER TABLE t REPLACE PARTITION ID 'all' FROM tmp")
	err := commandsForTest(sink).executeAndFenceCommand(context.Background(), "unused", command)
	require.ErrorIs(test, err, lostReply)
	require.Equal(test, 2, calls)
}

func TestDropRetriesOnlyUnsupportedSizeSetting(test *testing.T) {
	sink := newDatabaseFixture(test)
	calls := 0
	sizeLimit := xerrors.New("table exceeds server drop limit")
	configureTestNodes(sink, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		nodes[0][0].HTTP = newSessionHTTPClient(
			func(params url.Values) error {
				calls++
				switch calls {
				case 1:
					require.Equal(test, "0", params.Get("max_table_size_to_drop"))
					return &clickhouse_go.Exception{Code: unknownSettingCode, Message: "Unknown setting max_table_size_to_drop"}
				case 2:
					require.Empty(test, params.Get("max_table_size_to_drop"))
					return sizeLimit
				default:
					return &clickhouse_go.Exception{Code: sessionNotFoundCode, Message: "Session not found"}
				}
			},
		)

		return nodes
	},
	)
	command := ch_eo_common.NewCommand("session", "DROP TABLE tmp SYNC")
	err := commandsForTest(sink).executeAndFenceCommand(context.Background(), "unused", command)
	require.ErrorIs(test, err, sizeLimit)
	require.Equal(test, 3, calls)
}

func TestReplicatedDatabaseQuerySettings(test *testing.T) {
	sink := newDatabaseFixture(test)
	sink.databaseOperations = newReplicated(sink.logger, sink.transferID, sink.dstConfig.Database, sink.dstConfig.Cleanup, sink.tablesClient, sink.table.Snapshot(), testCheckpoint(sink.table))
	configureTestNodes(sink, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		nodes[0][0].HTTP = newSessionHTTPClient(
			func(params url.Values) error {
				require.Equal(test, "0", params.Get("database_replicated_allow_only_replicated_engine"))
				require.Equal(test, "1", params.Get("database_replicated_allow_explicit_uuid"))
				require.Equal(test, "1", params.Get("database_replicated_allow_replicated_engine_arguments"))
				require.Equal(test, "none", params.Get("distributed_ddl_output_mode"))
				return nil
			},
		)

		return nodes
	},
	)
	require.NoError(test, commandsForTest(sink).executeCommandOverHTTP(context.Background(), "unused", "CREATE TABLE tmp (id UInt64) ENGINE=MergeTree ORDER BY id", nil))
	for _, code := range []int32{164, 452, 241} {
		configureTestNodes(sink, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
			nodes[0][0].HTTP = newSessionHTTPClient(
				func(url.Values) error {
					return &clickhouse_go.Exception{Code: code, Message: "setting constraint"}
				},
			)

			return nodes
		},
		)
		err := commandsForTest(sink).executeCommandOverHTTP(context.Background(), "unused", "CREATE TABLE tmp (id UInt64) ENGINE=MergeTree ORDER BY id", nil)
		require.Error(test, err)
		require.Equal(test, code != 241, abstract.IsFatal(err))
	}
}

func TestReplicatedDatabaseFenceWaitsForDDL(test *testing.T) {
	sink, mock := newDatabaseReadFixture(test)
	mock.ExpectQuery("SELECT toUInt8").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
	sink.databaseOperations = newReplicated(sink.logger, sink.transferID, sink.dstConfig.Database, sink.dstConfig.Cleanup, sink.tablesClient, sink.table.Snapshot(), testCheckpoint(sink.table))
	ddlWaited := false
	configureTestNodes(sink, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		nodes[0][0].HTTP = newSessionHTTPClient(
			func(params url.Values) error {
				if params.Get("session_check") == "1" {
					return &clickhouse_go.Exception{Code: sessionNotFoundCode, Message: "Session not found"}
				}
				ddlWaited = true
				return xerrors.New("database replica is unavailable")
			},
		)

		return nodes
	},
	)
	command := ch_eo_common.NewCommand("session", "EXCHANGE TABLES old AND candidate")
	require.ErrorContains(test, commandsForTest(sink).fenceCommand(context.Background(), "unused", command), "database replica is unavailable")
	require.True(test, ddlWaited)
}

func TestCommandFenceDoesNotWaitForDDLBeforeSessionIsSettled(test *testing.T) {
	sink := newDatabaseFixture(test)
	sink.databaseOperations = newReplicated(sink.logger, sink.transferID, sink.dstConfig.Database, sink.dstConfig.Cleanup, sink.tablesClient, sink.table.Snapshot(), testCheckpoint(sink.table))
	connectionLost := xerrors.New("session status is unknown")
	ddlWaited := false
	configureTestNodes(sink, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		nodes[0][0].HTTP = newSessionHTTPClient(
			func(params url.Values) error {
				if params.Get("session_check") == "1" {
					return connectionLost
				}
				ddlWaited = true
				return nil
			},
		)

		return nodes
	},
	)
	command := ch_eo_common.NewCommand("session", "EXCHANGE TABLES old AND candidate")
	require.ErrorIs(test, commandsForTest(sink).fenceCommand(context.Background(), "unused", command), connectionLost)
	require.False(test, ddlWaited)
}

func TestCommandFencesOnlyReplicatedDatabaseDDL(test *testing.T) {
	for _, statement := range []string{
		"SELECT 1 FORMAT Null",
		"ALTER TABLE candidate ATTACH PARTITION ID 'all' FROM tmp",
		"CREATE TABLE tmp (id UInt64) ENGINE=MergeTree ORDER BY id",
		"ALTER TABLE candidate REPLACE PARTITION ID 'all' FROM tmp",
		"EXCHANGE TABLES old AND candidate",
		"RENAME TABLE candidate TO destination",
		"DROP TABLE tmp SYNC",
	} {
		test.Run(statement, func(test *testing.T) {
			for _, ambiguousReply := range []bool{false, true} {
				sink, mock := newDatabaseReadFixture(test)
				if !strings.HasPrefix(statement, "SELECT ") {
					mock.ExpectQuery("SELECT toUInt8").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
				}
				sink.databaseOperations = newReplicated(sink.logger, sink.transferID, sink.dstConfig.Database, sink.dstConfig.Cleanup, sink.tablesClient, sink.table.Snapshot(), testCheckpoint(sink.table))
				ddlWaits := 0
				queryCalls := 0
				lostReply := io.EOF
				configureTestNodes(sink, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
					nodes[0][0].HTTP = newSessionHTTPClient(
						func(params url.Values) error {
							if params.Get("query_id") != "" {
								queryCalls++
								if strings.HasPrefix(statement, "ALTER TABLE ") {
									require.Equal(test, "1", params.Get("replication_alter_partitions_sync"))
								} else {
									require.Empty(test, params.Get("replication_alter_partitions_sync"))
								}
								if ambiguousReply {
									return lostReply
								}
								return nil
							}
							if params.Get("session_check") == "1" {
								return &clickhouse_go.Exception{Code: sessionNotFoundCode, Message: "Session not found"}
							}
							ddlWaits++
							return nil
						},
					)

					return nodes
				},
				)
				err := commandsForTest(sink).executeAndFenceCommand(context.Background(), "unused", ch_eo_common.NewCommand("session", statement))
				if ambiguousReply {
					require.ErrorIs(test, err, lostReply)
				} else {
					require.NoError(test, err)
				}
				expectedWaits := 1
				if strings.HasPrefix(statement, "SELECT ") {
					expectedWaits = 0
				}
				require.Equal(test, 1, queryCalls)
				require.Equal(test, expectedWaits, ddlWaits)
			}
		})
	}
}

func TestStagingCommandRetriesOnlySessionContention(test *testing.T) {
	for _, scenario := range []string{"locked", "ambiguous"} {
		test.Run(scenario, func(test *testing.T) {
			sink := newDatabaseFixture(test)
			require.NoError(test, sink.table.RegisterStaging(context.Background(), "__test_", map[string]string{"unused": "staging-gate"}))
			calls := 0
			firstQueryID := ""
			configureTestNodes(sink, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
				nodes[0][0].HTTP = newSessionHTTPClient(
					func(params url.Values) error {
						calls++
						require.Equal(test, "staging-gate", params.Get("session_id"))
						require.Equal(test, "1", params.Get("session_check"))
						require.Empty(test, params.Get("close_session"))
						if calls == 1 {
							firstQueryID = params.Get("query_id")
							require.NotEmpty(test, firstQueryID)
							if scenario == "locked" {
								return &clickhouse_go.Exception{Code: sessionLockedCode}
							}
							return io.EOF
						}
						require.Equal(test, firstQueryID, params.Get("query_id"))
						return nil
					},
				)

				return nodes
			},
			)
			err := commandsForTest(sink).executeStagingCommand(context.Background(), "unused", "CREATE TABLE tmp (id UInt64) ENGINE=MergeTree ORDER BY id")
			if scenario == "locked" {
				require.NoError(test, err)
				require.Equal(test, 2, calls)
			} else {
				require.ErrorIs(test, err, io.EOF)
				require.Equal(test, 1, calls)
			}
		})
	}
}

func newIntegrationDatabase(test *testing.T) *databaseFixture {
	test.Helper()
	if os.Getenv("DT_SNAPSHOT_CLICKHOUSE_TEST") != "1" {
		test.Skip("requires a local ClickHouse recipe")
	}
	fixture := newDatabaseFixture(test)
	cfg := &clickhouse_model.ChDestination{Database: "default", User: "default", NativePort: 19000, HTTPPort: 18123, Cleanup: model.Drop}
	params, err := cfg.ToSinkParams(&model.Transfer{})
	require.NoError(test, err)
	host := &conn_clickhouse.Host{Name: "127.0.0.1", NativePort: 19000, HTTPPort: 18123}
	connection, err := conn.ConnectNative(host, params)
	require.NoError(test, err)
	cfg.Database = "dt_snapshot_commands_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = connection.Exec("CREATE DATABASE " + ch_eo_common.QuoteIdentifier(cfg.Database) + " ENGINE=Atomic")
	require.NoError(test, err)
	test.Cleanup(func() {
		_, err := connection.Exec("DROP DATABASE " + ch_eo_common.QuoteIdentifier(cfg.Database) + " SYNC")
		require.NoError(test, err)
		require.NoError(test, connection.Close())
	})
	params, err = cfg.ToSinkParams(&model.Transfer{})
	require.NoError(test, err)
	client, err := httpclient.NewHTTPClientImpl(params)
	require.NoError(test, err)
	fixture.dstConfig = cfg
	configureTestNodes(fixture, func([][]ch_eo_common.Node) [][]ch_eo_common.Node {
		return [][]ch_eo_common.Node{{ch_eo_common.NewNode(host, connection, client)}}
	})
	require.NoError(test, fixture.table.RegisterStaging(context.Background(), "__command_test_", nil))
	fixture.databaseOperations = fixture.databaseOperations.ForTable(fixture.table.Snapshot(), testCheckpoint(fixture.table))
	return fixture
}
func TestIntegrationSessionFenceRejectsLateRequest(test *testing.T) {
	sink := newIntegrationDatabase(test)
	ctx := context.Background()
	require.NoError(test, sink.tablesClient.CheckSessionFencingSupport(ctx, "127.0.0.1"))
	command, err := sink.tablesClient.PrepareSessionCommand(ctx, "127.0.0.1", "SELECT 1 FORMAT Null")
	require.NoError(test, err)
	require.NoError(test, commandsForTest(sink).executeAndFenceCommand(ctx, "127.0.0.1", command))
	err = sink.tablesClient.ExecuteCommandOverHTTP(ctx, "127.0.0.1", command.SQL, ch_eo_common.ExistingSessionParams(command.SessionID))
	var serverErr *clickhouse_go.Exception
	require.ErrorAs(test, err, &serverErr)
	require.EqualValues(test, sessionNotFoundCode, serverErr.Code)
}

func TestIntegrationOwnedResourceDrop(test *testing.T) {
	sink := newIntegrationDatabase(test)
	ctx := context.Background()
	resource := ch_eo_common.NewTemporaryTable(sink.table.TemporaryTablePrefix(), "127.0.0.1", ch_eo_common.RoleAssembly)
	ddl, err := ch_eo_common.BuildCreateTableDDL(sink.dstConfig.Database, resource.TableName, resource.ExpectedTableUUID, "CREATE TABLE t (id UInt64) ENGINE = MergeTree ORDER BY id", "")
	require.NoError(test, err)
	_, err = sink.tablesClient.NodesByShard()[0][0].DB.Exec(ddl)
	require.NoError(test, err)
	require.NoError(test, commandsForTest(sink).dropOwnedTemporaryTable(ctx, resource))
	actualUUID, err := sink.tablesClient.TableUUID(ctx, resource.CommandHostName, resource.TableName)
	require.NoError(test, err)
	require.Empty(test, actualUUID)
	require.NoError(test, commandsForTest(sink).DropTemporaryTables(ctx))
}

func TestIntegrationDropRetryWithSameSinkAfterLostReply(test *testing.T) {
	sink := newIntegrationDatabase(test)
	ctx := context.Background()
	resource := ch_eo_common.NewTemporaryTable(sink.table.TemporaryTablePrefix(), "127.0.0.1", ch_eo_common.RoleAssembly)
	ddl, err := ch_eo_common.BuildCreateTableDDL(sink.dstConfig.Database, resource.TableName, resource.ExpectedTableUUID, "CREATE TABLE t (id UInt64) ENGINE = MergeTree ORDER BY id", "")
	require.NoError(test, err)
	require.NoError(test, commandsForTest(sink).executeOwnedTableCommand(ctx, resource, ddl))
	injection := newLostHTTPReply(sink.tablesClient.NodesByShard()[0][0].HTTP, func(statement string) bool { return strings.HasPrefix(statement, "DROP TABLE ") })
	configureTestNodes(sink, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		nodes[0][0].HTTP = injection

		return nodes
	},
	)
	require.ErrorContains(test, commandsForTest(sink).dropOwnedTemporaryTable(ctx, resource), "injected lost HTTP command reply")
	require.True(test, injection.lost)
	require.NotEmpty(test, sink.table.PendingTableUUIDs())
	require.NoError(test, commandsForTest(sink).dropOwnedTemporaryTable(ctx, resource))
	require.Empty(test, sink.table.PendingTableUUIDs())
	actualUUID, err := sink.tablesClient.TableUUID(ctx, resource.CommandHostName, resource.TableName)
	require.NoError(test, err)
	require.Empty(test, actualUUID)
	require.NoError(test, commandsForTest(sink).DropTemporaryTables(ctx))
}

type lostHTTPReply struct {
	httpclient.HTTPClient
	matchStatement func(string) bool
	lost           bool
	cancel         context.CancelFunc
}

func newLostHTTPReply(client httpclient.HTTPClient, matchStatement func(string) bool) *lostHTTPReply {
	return &lostHTTPReply{
		HTTPClient:     client,
		matchStatement: matchStatement,
		lost:           false,
		cancel:         nil,
	}
}

func (c *lostHTTPReply) QueryStream(ctx context.Context, logger log.Logger, host *conn_clickhouse.Host, query any, params url.Values) (io.ReadCloser, error) {
	body, err := c.HTTPClient.QueryStream(ctx, logger, host, query, params)
	if err != nil {
		if body != nil {
			_ = body.Close()
		}
		return nil, err
	}
	if c.lost || !c.matchStatement(fmt.Sprint(query)) {
		return body, nil
	}
	_, readErr := io.Copy(io.Discard, body)
	closeErr := body.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	c.lost = true
	if c.cancel != nil {
		c.cancel()
	}
	return nil, xerrors.New("injected lost HTTP command reply")
}

// lostPublicationStateReply stores the publication decision, then loses its ACK.

func TestTableOperationsKeepIndependentPublicationOwners(test *testing.T) {
	fixture := newDatabaseFixture(test)
	ctx := context.Background()
	first := fixture.store.NewTable(abstract.TableID{Name: "first"}, "first", "", false)
	second := fixture.store.NewTable(abstract.TableID{Name: "second"}, "second", "", false)
	require.NoError(test, first.RegisterStaging(ctx, "__first_", map[string]string{"unused": "first-session"}))
	require.NoError(test, second.RegisterStaging(ctx, "__second_", map[string]string{"unused": "second-session"}))
	var fencedSessions []string
	configureTestNodes(fixture, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		nodes[0][0].HTTP = newSessionHTTPClient(func(params url.Values) error {
			fencedSessions = append(fencedSessions, params.Get("session_id"))
			return &clickhouse_go.Exception{Code: sessionNotFoundCode, Message: "Session not found"}
		})
		return nodes
	})
	firstOperations := fixture.databaseOperations.ForTable(first.Snapshot(), testCheckpoint(first))
	secondOperations := fixture.databaseOperations.ForTable(second.Snapshot(), testCheckpoint(second))
	require.NoError(test, firstOperations.CloseStaging(ctx))
	require.False(test, first.StagingClosed()) // SQL operations do not persist state.
	require.NoError(test, first.CloseStaging(ctx))
	require.True(test, first.StagingClosed())
	require.False(test, second.StagingClosed())
	require.Equal(test, []string{"first-session"}, fencedSessions)
	require.NoError(test, secondOperations.CloseStaging(ctx))
	require.Equal(test, []string{"first-session", "second-session"}, fencedSessions)
}

func testCheckpoint(table *transfer_state.TableState) CommandCheckpoint {
	return func(ctx context.Context, hostName string, ownedTable *ch_eo_common.OwnedTable, command *ch_eo_common.Command) error {
		if ownedTable == nil {
			return table.RecordPublicationCommand(ctx, hostName, command)
		}
		if command == nil {
			return table.ClearPendingTableCommand(ctx, ownedTable.ExpectedTableUUID)
		}
		return table.RecordPendingTableCommand(ctx, ownedTable, command)
	}
}

func TestCommandCheckpointPrecedesExecution(test *testing.T) {
	for _, failCheckpoint := range []bool{false, true} {
		test.Run(fmt.Sprintf("failure=%t", failCheckpoint), func(test *testing.T) {
			fixture := newDatabaseFixture(test)
			checkpointError := xerrors.New("lost checkpoint reply")
			var events []string
			configureTestNodes(fixture, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
				nodes[0][0].HTTP = newSessionHTTPClient(func(params url.Values) error {
					if params.Get("session_check") == "1" && params.Get("query_id") == "" {
						return &clickhouse_go.Exception{Code: sessionNotFoundCode}
					}
					if params.Get("query_id") != "" {
						events = append(events, "SQL")
					}
					return nil
				})
				return nodes
			})
			commands := commandsForTest(fixture)
			commands.checkpoint = func(_ context.Context, hostName string, ownedTable *ch_eo_common.OwnedTable, command *ch_eo_common.Command) error {
				require.Equal(test, "unused", hostName)
				require.Equal(test, "temporary", ownedTable.TableName)
				if command == nil {
					events = append(events, "resolve")
					return nil
				}
				events = append(events, "intent")
				if failCheckpoint {
					return checkpointError
				}
				return nil
			}
			temporaryTable := ch_eo_common.NewOwnedTable("unused", "temporary", "uuid")
			err := commands.executeOwnedTableCommand(context.Background(), temporaryTable, "CREATE TABLE temporary (id UInt64) ENGINE=MergeTree ORDER BY id")
			if failCheckpoint {
				require.ErrorIs(test, err, checkpointError)
				require.Equal(test, []string{"intent"}, events)
				require.True(test, commands.checkpointFailed())
				require.ErrorIs(test, commands.executeOwnedTableCommand(context.Background(), temporaryTable, "CREATE TABLE temporary (id UInt64) ENGINE=MergeTree ORDER BY id"), checkpointError)
				require.Equal(test, []string{"intent"}, events)
			} else {
				require.NoError(test, err)
				require.Equal(test, []string{"intent", "SQL", "resolve"}, events)
				require.Empty(test, commands.pendingTableUUIDs())
			}
		})
	}
}
