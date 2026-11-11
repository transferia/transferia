package clickhouse

import (
	"io"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/columntypes"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/topology"
	"github.com/transferia/transferia/pkg/stats"
)

var clusterInsertSchema = abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "id", DataType: "uint64", Required: true}})

func newInsertTestCluster(hostNames ...string) *sinkCluster {
	hosts := make([]*conn_clickhouse.Host, 0, len(hostNames))
	for _, hostName := range hostNames {
		hosts = append(hosts, &conn_clickhouse.Host{Name: hostName})
	}
	config := &clickhouse_model.ChDestination{Database: "db"}
	config.WithDefaults()
	return &sinkCluster{
		logger:                logger.Log,
		config:                config.ToReplicationFromPGSinkParams().MakeChildShardParams(hosts),
		topology:              topology.NewTopology("cluster", false),
		pool:                  &serverPool{newServer: nil, logger: logger.Log, counter: atomic.Uint64{}, mu: sync.RWMutex{}, servers: nil, closeCh: make(chan struct{}), closeOnce: sync.Once{}},
		distributedDDLMu:      sync.Mutex{},
		distributedDDLEnabled: nil,
	}
}

func newInsertTestServer(test *testing.T, cluster *sinkCluster, hostName string) (*SinkServer, sqlmock.Sqlmock) {
	test.Helper()
	database, queries, err := sqlmock.New()
	require.NoError(test, err)
	config := &clickhouse_model.ChDestination{Database: "db", IsSchemaMigrationDisabled: true}
	config.WithDefaults()
	params := config.ToReplicationFromPGSinkParams().MakeChildServerParams(&conn_clickhouse.Host{Name: hostName})
	server := NewSinkServerImpl(params, database, semver.MustParse("26.3.1"), time.UTC, logger.Log, stats.NewChStats(emptyRegistry()), cluster)
	test.Cleanup(func() {
		queries.ExpectClose()
		require.NoError(test, server.Close())
		require.NoError(test, queries.ExpectationsWereMet())
	})
	return server, queries
}

func cacheInsertTestTable(server *SinkServer, tableName string) {
	server.tables[tableName] = &sinkTable{
		server:               server,
		tableName:            tableName,
		config:               server.config,
		logger:               logger.Log,
		colTypes:             columntypes.TypeMapping{"id": columntypes.NewTypeDescription("UInt64")},
		cols:                 clusterInsertSchema,
		metrics:              server.metrics,
		avgRowSize:           0,
		cluster:              server.cluster,
		timezone:             time.UTC,
		version:              server.version,
		isReplicatedDatabase: false,
	}
}

func expectClusterInsert(queries sqlmock.Sqlmock, tableName string, commitError error) {
	query := regexp.QuoteMeta("INSERT INTO `db`.`" + tableName + "` (`id`) VALUES (?)")
	queries.ExpectBegin()
	queries.ExpectPrepare(query).ExpectExec().WithArgs(uint64(7)).WillReturnResult(sqlmock.NewResult(0, 1))
	commit := queries.ExpectCommit()
	if commitError != nil {
		commit.WillReturnError(commitError)
	}
}

func clusterInsertRows(tableName string) []abstract.ChangeItem {
	return []abstract.ChangeItem{{Kind: abstract.InsertKind, Table: tableName, TableSchema: clusterInsertSchema, ColumnNames: []string{"id"}, ColumnValues: []any{uint64(7)}}}
}

func TestClusterInsertPreservesRoundRobin(test *testing.T) {
	cluster := newInsertTestCluster("a", "b")
	first, firstQueries := newInsertTestServer(test, cluster, "a")
	second, secondQueries := newInsertTestServer(test, cluster, "b")
	cluster.pool.servers = []*SinkServer{first, second}
	cacheInsertTestTable(first, "t")
	cacheInsertTestTable(second, "t")

	// Only the next host's INSERT is expected; no table metadata reads are needed.
	for _, queries := range []sqlmock.Sqlmock{firstQueries, secondQueries, firstQueries, secondQueries} {
		expectClusterInsert(queries, "t", nil)
		require.NoError(test, cluster.Insert(&TableSpec{Name: "t", Schema: clusterInsertSchema}, clusterInsertRows("t")))
	}
	require.True(test, first.lastFail.IsZero())
	require.True(test, second.lastFail.IsZero())
}

func TestClusterInsertDoesNotReplayAmbiguousInsert(test *testing.T) {
	cluster := newInsertTestCluster("a", "b")
	first, firstQueries := newInsertTestServer(test, cluster, "a")
	second, secondQueries := newInsertTestServer(test, cluster, "b")
	cluster.pool.servers = []*SinkServer{first, second}
	cacheInsertTestTable(first, "t")
	cacheInsertTestTable(second, "t")
	expectClusterInsert(firstQueries, "t", io.ErrUnexpectedEOF)

	// B has no expectations until this call returns: an ambiguous INSERT on A
	// must not cause another write or metadata query on B in the same call.
	err := cluster.Insert(&TableSpec{Name: "t", Schema: clusterInsertSchema}, clusterInsertRows("t"))
	require.ErrorIs(test, err, io.ErrUnexpectedEOF)
	require.False(test, first.lastFail.IsZero())
	require.True(test, second.lastFail.IsZero())

	// A later caller retry can use B through the original pool selection.
	expectClusterInsert(secondQueries, "t", nil)
	require.NoError(test, cluster.Insert(&TableSpec{Name: "t", Schema: clusterInsertSchema}, clusterInsertRows("t")))
	require.True(test, second.lastFail.IsZero())
}
