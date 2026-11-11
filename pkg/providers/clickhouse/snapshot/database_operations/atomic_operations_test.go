package database_operations

import (
	"context"
	"database/sql"
	sql_driver "database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
	"github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/part_uploader"
	"github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/transfer_state"
	"go.ytsaurus.tech/library/go/core/log"
)

type shardBuildTable struct {
	uuid string
	rows int64
}

func newShardBuildTable(uuid string, rows int64) shardBuildTable {
	return shardBuildTable{uuid: uuid, rows: rows}
}

type shardBuildServer struct {
	httpclient.HTTPClient
	mu          sync.Mutex
	tables      map[string]shardBuildTable
	creates     []string
	attaches    []string
	failure     error
	unavailable bool
	failures    int
	entered     chan<- struct{}
	release     <-chan struct{}
}

func newShardBuildServer() *shardBuildServer {
	return &shardBuildServer{
		HTTPClient:  nil,
		mu:          sync.Mutex{},
		tables:      make(map[string]shardBuildTable),
		creates:     nil,
		attaches:    nil,
		failure:     nil,
		unavailable: false,
		failures:    0,
		entered:     nil,
		release:     nil,
	}
}

var shardBuildCreate = regexp.MustCompile("CREATE TABLE `test`\\.`([^`]+)` UUID '([^']+)'")
var shardBuildNames = regexp.MustCompile("`test`\\.`([^`]+)`")

func (s *shardBuildServer) QueryStream(ctx context.Context, _ log.Logger, _ *conn_clickhouse.Host, query any, params url.Values) (io.ReadCloser, error) {
	statement := query.(string)
	if s.unavailable {
		return nil, io.ErrUnexpectedEOF
	}
	if statement == "SELECT 1 FORMAT Null" && params.Get("session_check") == "1" {
		return nil, &clickhouse_go.Exception{Code: sessionNotFoundCode, Message: "absent"}
	}
	if strings.HasPrefix(statement, "CREATE TABLE ") {
		match := shardBuildCreate.FindStringSubmatch(statement)
		if len(match) != 3 {
			return nil, xerrors.Errorf("unexpected CREATE: %s", statement)
		}
		if s.entered != nil {
			select {
			case s.entered <- struct{}{}:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			select {
			case <-s.release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		s.mu.Lock()
		s.tables[match[1]] = newShardBuildTable(match[2], 0)
		s.creates = append(s.creates, match[1])
		s.mu.Unlock()
	}
	if strings.HasPrefix(statement, "ALTER TABLE ") {
		names := shardBuildNames.FindAllStringSubmatch(statement, -1)
		if len(names) != 2 {
			return nil, xerrors.Errorf("unexpected ALTER: %s", statement)
		}
		s.mu.Lock()
		dst := s.tables[names[0][1]]
		dst.rows += s.tables[names[1][1]].rows
		s.tables[names[0][1]] = dst
		s.attaches = append(s.attaches, names[0][1])
		// Lose a response after applying the data change, not before admission.
		fail := s.failures > 0 && strings.Contains(names[0][1], string(ch_eo_common.RoleAssembly))
		if fail {
			s.failures--
		}
		s.mu.Unlock()
		if fail {
			return nil, s.failure
		}
	}
	if strings.HasPrefix(statement, "DROP TABLE ") {
		names := shardBuildNames.FindStringSubmatch(statement)
		s.mu.Lock()
		delete(s.tables, names[1])
		s.mu.Unlock()
	}
	return io.NopCloser(strings.NewReader("")), nil
}

type shardBuildConnector struct{ server *shardBuildServer }

func newShardBuildConnector(server *shardBuildServer) shardBuildConnector {
	return shardBuildConnector{server: server}
}

func (c shardBuildConnector) Connect(context.Context) (sql_driver.Conn, error) {
	return newShardBuildConn(c.server), nil
}
func (c shardBuildConnector) Driver() sql_driver.Driver {
	return newShardBuildDriver()
}

type shardBuildDriver struct{}

func newShardBuildDriver() shardBuildDriver {
	return shardBuildDriver{}
}

func (d shardBuildDriver) Open(string) (sql_driver.Conn, error) {
	return nil, xerrors.New("use connector")
}

type shardBuildConn struct{ server *shardBuildServer }

func newShardBuildConn(server *shardBuildServer) shardBuildConn {
	return shardBuildConn{server: server}
}

func (c shardBuildConn) Prepare(string) (sql_driver.Stmt, error) {
	return nil, xerrors.New("unsupported")
}
func (c shardBuildConn) Close() error {
	return nil
}
func (c shardBuildConn) Begin() (sql_driver.Tx, error) {
	return nil, xerrors.New("unsupported")
}
func (c shardBuildConn) QueryContext(_ context.Context, statement string, args []sql_driver.NamedValue) (sql_driver.Rows, error) {
	c.server.mu.Lock()
	defer c.server.mu.Unlock()

	if c.server.unavailable {
		return nil, io.ErrUnexpectedEOF
	}
	switch {
	case strings.HasPrefix(statement, "SELECT toUInt8"):
		return newShardBuildRows(int64(1)), nil
	case strings.HasPrefix(statement, "SELECT name FROM system.tables"):
		var names []sql_driver.Value
		for name := range c.server.tables {
			if strings.HasSuffix(name, "_ready") {
				names = append(names, name)
			}
		}
		return newShardBuildRows(names...), nil
	case strings.Contains(statement, "toString(uuid)"):
		table, found := c.server.tables[args[1].Value.(string)]
		if !found {
			return newShardBuildRows(), nil
		}
		return newShardBuildRows(table.uuid), nil
	case strings.HasPrefix(statement, "SELECT sum(rows) FROM system.parts"):
		name := args[1].Value.(string)
		return newShardBuildRows(c.server.tables[name].rows), nil
	case strings.Contains(statement, "SELECT DISTINCT partition_id"):
		return newShardBuildRows("all"), nil
	default:
		return nil, xerrors.Errorf("unexpected read: %s", statement)
	}
}

type shardBuildRows struct{ values []sql_driver.Value }

func newShardBuildRows(values ...sql_driver.Value) *shardBuildRows {
	return &shardBuildRows{values: values}
}

func (r *shardBuildRows) Columns() []string {
	return []string{"value"}
}
func (r *shardBuildRows) Close() error {
	return nil
}
func (r *shardBuildRows) Next(dst []sql_driver.Value) error {
	if r.values == nil {
		return io.EOF
	}
	copy(dst, r.values)
	r.values = nil
	return nil
}

func newShardBuildTest(test *testing.T, storageOverride ...coordinator.TransferState) (*databaseFixture, *transfer_state.TableState, []ch_eo_common.ReadyPart, []*shardBuildServer) {
	test.Helper()
	fixture := newDatabaseFixture(test)
	if len(storageOverride) != 0 {
		fixture.storage = storageOverride[0]
		fixture.store = transfer_state.NewState(fixture.storage, "transfer", "operation")
	}
	table := fixture.store.NewTable(abstract.TableID{Name: "t"}, "t", "CREATE TABLE t (id UInt64) ENGINE = MergeTree ORDER BY id", false)
	fixture.table = table
	nodes := make([][]ch_eo_common.Node, 2)
	servers := make([]*shardBuildServer, 2)
	prefix := "__build_"
	attemptKey := ch_eo_common.Murmur128Hex("part") + "_" + ch_eo_common.Murmur128Hex("attempt")
	readyName := prefix + attemptKey + "_ready"
	for shardIndex := range servers {
		server := newShardBuildServer()
		servers[shardIndex] = server
		host := fmt.Sprintf("host%d", shardIndex)
		connection := sql.OpenDB(newShardBuildConnector(server))
		test.Cleanup(func() { require.NoError(test, connection.Close()) })
		nodes[shardIndex] = []ch_eo_common.Node{ch_eo_common.NewNode(&conn_clickhouse.Host{Name: host}, connection, server)}
		table.AddReplica(host, shardIndex, "")
		server.tables[readyName] = newShardBuildTable(ch_eo_common.PartTableUUID(prefix, attemptKey, host), 2)
	}
	require.NoError(test, table.RegisterStaging(context.Background(), prefix, nil))
	configureTestNodes(fixture, func([][]ch_eo_common.Node) [][]ch_eo_common.Node { return nodes })
	parts, err := part_uploader.DiscoverReady(context.Background(), fixture.tablesClient, fixture.databaseOperations.ForTable(table.Snapshot(), testCheckpoint(table)), fixture.dstConfig.Database, table.Snapshot())
	require.NoError(test, err)
	return fixture, table, parts, servers
}

func TestCandidateBuildRunsAllShardsConcurrently(test *testing.T) {
	sink, state, selected, servers := newShardBuildTest(test)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	for _, server := range servers {
		server.entered = entered
		server.release = release
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result := make(chan error, 1)
	go func() { result <- sink.buildCandidateTable(ctx, state, selected) }()
	for range servers {
		select {
		case <-entered:
		case <-ctx.Done():
			close(release)
			<-result
			test.Fatal("shard builds did not overlap")
		}
	}
	// Both CREATEs are durably registered while their HTTP requests are blocked.
	pendingCount := len(state.PendingTableUUIDs())
	require.Equal(test, 2, pendingCount)
	close(release)
	require.NoError(test, <-result)
	require.Empty(test, state.PendingTableUUIDs())
	require.False(test, state.PublicationDecided())
	for _, node := range state.Replicas() {
		require.NoError(test, sink.tablesClient.CheckTableUUIDAndRowCount(ctx, node.HostName, node.CandidateTable.TableName, node.CandidateTable.ExpectedTableUUID, 2))
	}
}

func TestCandidateBuildRetriesOnlyFailedShardWithFreshAssembly(test *testing.T) {
	for _, failures := range []int{1, 3} {
		test.Run(fmt.Sprint(failures), func(test *testing.T) {
			sink, state, selected, servers := newShardBuildTest(test)
			servers[0].failure = io.ErrUnexpectedEOF
			servers[0].failures = failures
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			err := sink.buildCandidateTable(ctx, state, selected)
			if failures == 1 {
				require.NoError(test, err)
				require.EqualValues(test, 2, state.Replicas()[0].ExpectedRowCount)
			} else {
				require.ErrorIs(test, err, io.ErrUnexpectedEOF)
			}
			require.False(test, state.PublicationDecided())
			// The successful shard is never rebuilt. Each retry uses a new assembly.
			require.Len(test, servers[1].creates, 2)
			var assemblies []string
			for _, name := range servers[0].creates {
				if strings.Contains(name, string(ch_eo_common.RoleAssembly)) {
					assemblies = append(assemblies, name)
				}
			}
			expected := failures + 1
			if expected > 3 {
				expected = 3
			}
			require.Len(test, assemblies, expected)
			expectedAttaches := expected
			if failures == 1 {
				expectedAttaches++ // Successful assembly is also imported into its candidate.
			}
			require.Len(test, servers[0].attaches, expectedAttaches)
			seen := make(map[string]bool)
			for _, name := range assemblies {
				require.False(test, seen[name])
				seen[name] = true
			}
		})
	}
}

func TestCandidateBuildDoesNotRetryPermanentError(test *testing.T) {
	sink, state, selected, servers := newShardBuildTest(test)
	servers[0].failure = &clickhouse_go.Exception{Code: 516, Message: "authentication failed"}
	servers[0].failures = 3
	require.Error(test, sink.buildCandidateTable(context.Background(), state, selected))
	require.Len(test, servers[0].creates, 1)
	require.False(test, state.PublicationDecided())
}

func TestCandidateBuildCancellationPreservesEveryPendingCommand(test *testing.T) {
	sink, state, selected, servers := newShardBuildTest(test)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	for _, server := range servers {
		server.entered = entered
		server.release = release
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result := make(chan error, 1)
	go func() { result <- sink.buildCandidateTable(ctx, state, selected) }()
	for range servers {
		select {
		case <-entered:
		case <-ctx.Done():
			<-result
			test.Fatal("build did not start")
		}
	}
	cancel()
	require.Error(test, <-result)
	require.False(test, state.PublicationDecided())
	// A new process must see both registrations, even though it never saw the workers.
	restored, err := sink.store.LoadTable(context.Background(), state.StateKey())
	require.NoError(test, err)
	require.NotNil(test, restored)
	require.Len(test, restored.PendingTableUUIDs(), 2)
	require.NoError(test, sink.databaseOperations.ForTable(restored.Snapshot(), testCheckpoint(restored)).WaitForPendingCommandsAndClearRecords(context.Background()))
	require.Empty(test, restored.PendingTableUUIDs())
}

func TestCandidateBuildStopsAfterLostStateAcknowledgement(test *testing.T) {
	storage := newLostTableStateReply()
	storage.failWrites = false
	sink, state, selected, servers := newShardBuildTest(test, storage)
	storage.failWrites = true
	storage.writes = 0
	require.Error(test, sink.buildCandidateTable(context.Background(), state, selected))
	require.Equal(test, 1, storage.writes)
	for _, server := range servers {
		require.Empty(test, server.creates)
	}
	require.False(test, state.PublicationDecided())
	recovery := transfer_state.NewState(storage, "transfer", "operation")
	restored, err := recovery.LoadTable(context.Background(), state.StateKey())
	require.NoError(test, err)
	require.NotNil(test, restored)
	require.Len(test, restored.PendingTableUUIDs(), 1)
}

// serialState models a remote JSON store, rather than retaining pointers into
// the running sink. Failed state writes must not leak through pointer aliases.
type serialState struct {
	coordinator.TransferState
	data map[string]json.RawMessage
}

func newSerialState() *serialState {
	return &serialState{
		TransferState: nil,
		data:          make(map[string]json.RawMessage),
	}
}

func (s *serialState) GetTransferStateKeys(string) ([]string, error) {
	var result []string
	for key := range s.data {
		result = append(result, key)
	}
	return result, nil
}

func (s *serialState) GetTransferStateByKeys(_ string, keys []string) (map[string]*coordinator.TransferStateData, error) {
	result := make(map[string]*coordinator.TransferStateData)
	for _, key := range keys {
		if raw, ok := s.data[key]; ok {
			var value coordinator.TransferStateData
			if err := json.Unmarshal(raw, &value); err != nil {
				return nil, err
			}
			result[key] = &value
		}
	}
	return result, nil
}

func (s *serialState) SetTransferState(_ string, values map[string]*coordinator.TransferStateData) error {
	for key, value := range values {
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		s.data[key] = raw
	}
	return nil
}

func (s *serialState) RemoveTransferState(_ string, keys []string) error {
	for _, key := range keys {
		delete(s.data, key)
	}
	return nil
}

// lostTableStateReply stores the request but loses its acknowledgement.
type lostTableStateReply struct {
	*serialState
	writes     int
	failWrites bool
}

func newLostTableStateReply() *lostTableStateReply {
	return &lostTableStateReply{serialState: newSerialState(), writes: 0, failWrites: true}
}

func (s *lostTableStateReply) SetTransferState(transferID string, values map[string]*coordinator.TransferStateData) error {
	s.writes++
	if err := s.serialState.SetTransferState(transferID, values); err != nil {
		return err
	}
	if s.failWrites {
		return errors.New("lost state acknowledgement")
	}
	return nil
}

func TestAtomicAvailableReplicaPublication(test *testing.T) {
	for _, scenario := range []string{"success", "incorrect_count", "all_unavailable", "permission_error", "unverified_candidate"} {
		test.Run(scenario, func(test *testing.T) {
			fixture, primary := newDatabaseReadFixture(test)
			configureTestNodes(fixture, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
				nodes[0][0].HTTP = newSessionHTTPClient(func(url.Values) error { return nil })
				return nodes
			})
			database, secondary, err := sqlmock.New()
			require.NoError(test, err)
			test.Cleanup(func() { require.NoError(test, secondary.ExpectationsWereMet()); _ = database.Close() })
			configureTestNodes(fixture, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
				nodes[0] = append(nodes[0], ch_eo_common.NewNode(&conn_clickhouse.Host{Name: "secondary"}, database, nil))
				return nodes
			})
			fixture.tablesClient.SetReplicaFailureGrace(0)
			table := ch_eo_common.NewSnapshotTable(abstract.TableID{Name: "events"}, "events", "", true)
			candidate := ch_eo_common.NewTemporaryTable("__test_", "unused", ch_eo_common.RoleCandidate)
			table.NodeStates = []ch_eo_common.NodeState{ch_eo_common.NewCandidate("unused", 0, *candidate, 10)}
			if scenario == "unverified_candidate" {
				table.NodeStates[0].CandidateTable.ExpectedTableUUID = ""
			}
			operations := newAtomic(fixture.logger, fixture.transferID, fixture.dstConfig.Database, fixture.dstConfig.Cleanup, fixture.tablesClient, table, func(context.Context, string, *ch_eo_common.OwnedTable, *ch_eo_common.Command) error { return nil })
			secondary.ExpectQuery("SELECT toUInt8").WillReturnError(io.ErrUnexpectedEOF)
			switch scenario {
			case "all_unavailable":
				primary.ExpectQuery("SELECT toUInt8").WillReturnError(io.ErrUnexpectedEOF)
			case "permission_error":
				primary.ExpectQuery("SELECT toUInt8").WillReturnError(&clickhouse_go.Exception{Code: 497, Message: "access denied"})
			default:
				primary.ExpectQuery("SELECT toUInt8").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
				if scenario != "unverified_candidate" {
					primary.ExpectQuery("SELECT toString").WillReturnRows(sqlmock.NewRows([]string{"uuid"}).AddRow(candidate.ExpectedTableUUID))
					primary.ExpectQuery("SELECT toString").WillReturnRows(sqlmock.NewRows([]string{"uuid"}).AddRow(candidate.ExpectedTableUUID))
					rows := 10
					if scenario == "incorrect_count" {
						rows = 9
					}
					primary.ExpectQuery("SELECT sum").WillReturnRows(sqlmock.NewRows([]string{"sum(rows)"}).AddRow(rows))
				}
			}
			err = operations.Publish(context.Background())
			if scenario == "success" {
				require.NoError(test, err)
			} else {
				require.Error(test, err)
			}
		})
	}
}

func TestAtomicReplicaModeAndTopology(test *testing.T) {
	fixture := newDatabaseFixture(test)
	operations := fixture.databaseOperations.(*atomicOperations)
	require.ErrorContains(test, operations.validateTopology([][]ch_eo_common.Node{{}}), "no available replica")
	operations.requireAllReplicas = false
	require.True(test, operations.IsReplicaCountCompatibleWithTopology(3, 3, 1))
	require.False(test, operations.IsReplicaCountCompatibleWithTopology(4, 3, 1))
	require.False(test, operations.IsReplicaCountCompatibleWithTopology(0, 3, 1))
	params := make(url.Values)
	operations.configureQueryParams("ALTER TABLE t ATTACH PARTITION 1", params)
	require.Equal(test, "1", params.Get("replication_alter_partitions_sync"))
	require.False(test, operations.ForTable(fixture.table.Snapshot(), testCheckpoint(fixture.table)).(*atomicOperations).requireAllReplicas)
	operations.requireAllReplicas = true
	operations.configureQueryParams("ALTER TABLE t ATTACH PARTITION 1", params)
	require.Equal(test, "2", params.Get("replication_alter_partitions_sync"))
	require.True(test, operations.ForTable(fixture.table.Snapshot(), testCheckpoint(fixture.table)).(*atomicOperations).requireAllReplicas)
}

func TestAtomicBuildCandidatesSkipsUnavailableReplicas(test *testing.T) {
	fixture, table, parts, _ := newShardBuildTest(test)
	configureTestNodes(fixture, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		for shardIndex := range nodes {
			server := newShardBuildServer()
			server.unavailable = true
			connection := sql.OpenDB(newShardBuildConnector(server))
			test.Cleanup(func() { require.NoError(test, connection.Close()) })
			hostName := fmt.Sprintf("unavailable-%d", shardIndex)
			nodes[shardIndex] = append(nodes[shardIndex], ch_eo_common.NewNode(&conn_clickhouse.Host{Name: hostName}, connection, server))
			table.AddReplica(hostName, shardIndex, "")
		}
		return nodes
	})
	fixture.tablesClient.SetReplicaFailureGrace(0)
	operations := fixture.databaseOperations.(*atomicOperations)
	operations.requireAllReplicas = false
	candidates, err := operations.ForTable(table.Snapshot(), testCheckpoint(table)).BuildCandidates(context.Background(), parts)
	require.NoError(test, err)
	require.Len(test, candidates, 2)
	for shardIndex, candidate := range candidates {
		require.Equal(test, shardIndex, candidate.ShardIndex)
		require.Equal(test, fmt.Sprintf("host%d", shardIndex), candidate.HostName)
		require.EqualValues(test, 2, candidate.ExpectedRowCount)
	}
}

func TestAtomicCleanupSkipsUnavailableCommandOwner(test *testing.T) {
	fixture, primary := newDatabaseReadFixture(test)
	configureTestNodes(fixture, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		nodes[0][0].HTTP = newSessionHTTPClient(func(url.Values) error { return nil })
		server := newShardBuildServer()
		server.unavailable = true
		connection := sql.OpenDB(newShardBuildConnector(server))
		test.Cleanup(func() { require.NoError(test, connection.Close()) })
		nodes[0] = append(nodes[0], ch_eo_common.NewNode(&conn_clickhouse.Host{Name: "unavailable"}, connection, server))
		return nodes
	})
	fixture.tablesClient.SetReplicaFailureGrace(0)
	table := fixture.table.Snapshot()
	resource := ch_eo_common.NewTemporaryTable("__test_", "unavailable", ch_eo_common.RoleCandidate)
	resource.PendingCommand = ch_eo_common.NewCommand("session", "CREATE TABLE temporary")
	table.PendingTableCommands = map[string]*ch_eo_common.OwnedTable{resource.ExpectedTableUUID: resource}
	checkpointCalled := false
	operations := newAtomic(fixture.logger, fixture.transferID, fixture.dstConfig.Database, fixture.dstConfig.Cleanup, fixture.tablesClient, table, func(context.Context, string, *ch_eo_common.OwnedTable, *ch_eo_common.Command) error {
		checkpointCalled = true
		return xerrors.New("must not erase an unresolved command")
	})
	primary.ExpectQuery("SELECT toUInt8").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
	require.NoError(test, operations.WaitForPendingCommandsAndClearRecords(context.Background()))
	require.False(test, checkpointCalled)
	require.Contains(test, table.PendingTableCommands, resource.ExpectedTableUUID)
}

func TestLoadingTableCreationRunsAllShardsConcurrently(test *testing.T) {
	fixture, table, _, servers := newShardBuildTest(test)
	entered := make(chan struct{}, len(servers))
	release := make(chan struct{})
	for _, server := range servers {
		server.entered = entered
		server.release = release
	}
	snapshotTable := table.Snapshot()
	snapshotTable.StagingDDLSessionIDs = map[string]string{"host0": "session0", "host1": "session1"}
	commands := fixture.databaseOperations.ForTable(snapshotTable, testCheckpoint(table))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		tables, err := commands.PrepareLoadingTables(ctx, ch_eo_common.Murmur128Hex("next-part"))
		if err == nil && len(tables) != len(servers) {
			err = xerrors.New("missing loading tables")
		}
		result <- err
	}()
	for range servers {
		select {
		case <-entered:
		case <-ctx.Done():
			close(release)
			<-result
			test.Fatal("loading table creates did not overlap")
		}
	}
	select {
	case err := <-result:
		close(release)
		test.Fatalf("preparation returned before CREATE completed: %v", err)
	default:
	}
	close(release)
	require.NoError(test, <-result)
}
