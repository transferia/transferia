package database_operations

import (
	"context"
	"io"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
	"go.ytsaurus.tech/library/go/core/log"
)

type mergeBuildHTTP struct {
	httpclient.HTTPClient
	server       *shardBuildServer
	mutex        sync.Mutex
	stopped      map[string]bool
	started      map[string]bool
	loseCopyRows bool
	loseStart    bool
	mergeOnStart bool
}

func newMergeBuildHTTP(server *shardBuildServer) *mergeBuildHTTP {
	return &mergeBuildHTTP{
		HTTPClient:   nil,
		server:       server,
		mutex:        sync.Mutex{},
		stopped:      make(map[string]bool),
		started:      make(map[string]bool),
		loseCopyRows: false,
		loseStart:    false,
		mergeOnStart: false,
	}
}

func (s *mergeBuildHTTP) QueryStream(ctx context.Context, logger log.Logger, host *conn_clickhouse.Host, query any, params url.Values) (io.ReadCloser, error) {
	statement := query.(string)
	result, err := s.server.QueryStream(ctx, logger, host, query, params)
	if err != nil {
		return nil, err
	}
	names := shardBuildNames.FindAllStringSubmatch(statement, -1)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	switch {
	case strings.HasPrefix(statement, "SYSTEM STOP MERGES "):
		s.stopped[names[0][1]] = true
	case strings.HasPrefix(statement, "SYSTEM START MERGES "):
		s.started[names[0][1]] = true
		s.stopped[names[0][1]] = false
		if s.mergeOnStart {
			s.server.mu.Lock()
			table := s.server.tables[names[0][1]]
			table.rows--
			s.server.tables[names[0][1]] = table
			s.server.mu.Unlock()
		}
		if s.loseStart {
			_ = result.Close()
			return nil, io.ErrUnexpectedEOF
		}
	case strings.HasPrefix(statement, "ALTER TABLE "):
		// A merge before validation can reduce a Replacing table's count.
		// A short copy is still an error even when merges have been stopped.
		if !s.stopped[names[0][1]] || s.loseCopyRows {
			s.server.mu.Lock()
			table := s.server.tables[names[0][1]]
			table.rows--
			s.server.tables[names[0][1]] = table
			s.server.mu.Unlock()
		}
	case strings.HasPrefix(statement, "RENAME TABLE "):
		s.server.mu.Lock()
		s.server.tables[names[1][1]] = s.server.tables[names[0][1]]
		delete(s.server.tables, names[0][1])
		s.server.mu.Unlock()
	}
	return result, nil
}

func TestPrivateCandidateBuildStopsMergesBeforeCopy(test *testing.T) {
	for _, testCase := range []struct {
		name     string
		loseRows bool
	}{
		{name: "complete_copy", loseRows: false},
		{name: "missing_rows", loseRows: true},
	} {
		test.Run(testCase.name, func(test *testing.T) {
			fixture, table, parts, servers := newShardBuildTest(test)
			table.UseDefinition("CREATE TABLE t (id UInt64) ENGINE=ReplacingMergeTree() ORDER BY id", false)
			clients := make([]*mergeBuildHTTP, len(servers))
			configureTestNodes(fixture, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
				for shardIndex, server := range servers {
					clients[shardIndex] = newMergeBuildHTTP(server)
					clients[shardIndex].loseCopyRows = testCase.loseRows
					clients[shardIndex].mergeOnStart = true
					nodes[shardIndex][0].HTTP = clients[shardIndex]
				}
				return nodes
			})
			err := fixture.buildCandidateTable(context.Background(), table, parts)
			if testCase.loseRows {
				var mismatch *ch_eo_common.RowCountMismatch
				require.ErrorAs(test, err, &mismatch)
				require.True(test, mismatch.HasMissingRows())
				for _, node := range table.Replicas() {
					require.Empty(test, node.CandidateTable.ExpectedTableUUID)
				}
			} else {
				require.NoError(test, err)
				for shardIndex, server := range servers {
					var stoppedNames []string
					for name := range clients[shardIndex].stopped {
						stoppedNames = append(stoppedNames, name)
					}
					require.ElementsMatch(test, server.creates, stoppedNames)
					require.Len(test, stoppedNames, 2, "both assembly and candidate must stop merges")
					for _, node := range table.Replicas() {
						if node.ShardIndex == shardIndex {
							require.True(test, clients[shardIndex].started[node.CandidateTable.TableName])
							require.False(test, clients[shardIndex].stopped[node.CandidateTable.TableName])
						}
					}
				}
			}
			require.False(test, table.PublicationDecided())
		})
	}
}

func TestCandidateReleaseFailureDoesNotRecordVerifiedGeneration(test *testing.T) {
	fixture, table, parts, servers := newShardBuildTest(test)
	clients := make([]*mergeBuildHTTP, len(servers))
	configureTestNodes(fixture, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		for shardIndex, server := range servers {
			clients[shardIndex] = newMergeBuildHTTP(server)
			nodes[shardIndex][0].HTTP = clients[shardIndex]
		}
		return nodes
	})
	clients[0].loseStart = true
	err := fixture.buildCandidateTable(context.Background(), table, parts)
	require.ErrorIs(test, err, io.ErrUnexpectedEOF)
	require.False(test, table.PublicationDecided())
	for _, node := range table.Replicas() {
		require.Empty(test, node.CandidateTable.ExpectedTableUUID)
	}
	pending := table.PendingTableUUIDs()
	require.Len(test, pending, 1)
	require.Contains(test, table.TableWithPendingCommand(pending[0]).PendingCommand.SQL, "SYSTEM START MERGES")
}

func TestDecidedPublicationRecoversAfterMergesAndRestart(test *testing.T) {
	fixture, table, parts, servers := newShardBuildTest(test)
	table.UseDefinition("CREATE TABLE t (id UInt64) ENGINE=ReplacingMergeTree() ORDER BY id", false)
	clients := make([]*mergeBuildHTTP, len(servers))
	configureTestNodes(fixture, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		for shardIndex, server := range servers {
			clients[shardIndex] = newMergeBuildHTTP(server)
			nodes[shardIndex][0].HTTP = clients[shardIndex]
		}
		return nodes
	})
	ctx := context.Background()
	require.NoError(test, fixture.buildCandidateTable(ctx, table, parts))
	require.NoError(test, table.DecidePublication(ctx))
	for shardIndex, node := range table.Replicas() {
		// A ClickHouse restart loses STOP MERGES. The verified data may then
		// be replaced/collapsed while the durable decision is being recovered.
		require.True(test, clients[shardIndex].started[node.CandidateTable.TableName])
		clients[shardIndex].stopped = make(map[string]bool)
		servers[shardIndex].mu.Lock()
		candidate := servers[shardIndex].tables[node.CandidateTable.TableName]
		candidate.rows = 1
		servers[shardIndex].tables[node.CandidateTable.TableName] = candidate
		servers[shardIndex].mu.Unlock()
	}
	restored, err := fixture.store.LoadTable(ctx, table.StateKey())
	require.NoError(test, err)
	operations := newAtomic(fixture.logger, fixture.transferID, fixture.dstConfig.Database, fixture.dstConfig.Cleanup, fixture.tablesClient, restored.Snapshot(), testCheckpoint(restored))
	require.NoError(test, operations.Publish(ctx))
	for _, node := range restored.Replicas() {
		require.NoError(test, fixture.tablesClient.CheckTableUUIDAndRowCount(ctx, node.HostName, "t", node.CandidateTable.ExpectedTableUUID, 1))
	}
	restored, err = fixture.store.LoadTable(ctx, table.StateKey())
	require.NoError(test, err)
	for _, node := range restored.Replicas() {
		require.Nil(test, node.PendingPublicationCommand)
	}
}

func TestReplicatedPublicationRecoveryChecksIdentityWithoutPhysicalCount(test *testing.T) {
	for _, status := range []string{ch_eo_common.PublicationDecided, ch_eo_common.PublicationComplete} {
		test.Run(status, func(test *testing.T) {
			fixture, database := newDatabaseReadFixture(test)
			database.MatchExpectationsInOrder(false)
			configureTestNodes(fixture, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
				nodes[0][0].HTTP = newSessionHTTPClient(func(url.Values) error { return nil })
				return nodes
			})
			table := ch_eo_common.NewSnapshotTable(abstract.TableID{Name: "events"}, "events", "", false)
			table.PublicationStatus = status
			candidate := ch_eo_common.NewOwnedTable("unused", "candidate", "verified-candidate")
			table.NodeStates = []ch_eo_common.NodeState{ch_eo_common.NewCandidate("unused", 0, *candidate, 2)}
			for range 4 {
				database.ExpectQuery("SELECT toUInt8").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
			}
			for range 2 {
				database.ExpectQuery("SELECT toString").WillReturnRows(sqlmock.NewRows([]string{"uuid"}).AddRow(candidate.ExpectedTableUUID))
			}
			// No physical count is expected: a recorded complete candidate may
			// already have been legitimately replaced/collapsed after a restart.
			operations := newReplicated(fixture.logger, fixture.transferID, fixture.dstConfig.Database, fixture.dstConfig.Cleanup, fixture.tablesClient, table, nil)
			require.NoError(test, operations.Publish(context.Background()))
		})
	}
}
