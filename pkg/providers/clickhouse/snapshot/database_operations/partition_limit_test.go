package database_operations

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
	"go.ytsaurus.tech/library/go/core/log"
)

type partitionLimitHTTPClient struct {
	httpclient.HTTPClient
}

func (c *partitionLimitHTTPClient) QueryStream(ctx context.Context, logger log.Logger, host *conn_clickhouse.Host, query any, params url.Values) (io.ReadCloser, error) {
	if statement, ok := query.(string); ok && strings.HasPrefix(statement, "INSERT INTO ") {
		// Keep the normal server cap and combine the source partitions in one
		// stream. The production copy must lift the cap before inserting.
		params.Set("max_partitions_per_insert_block", "100")
		params.Set("max_threads", "1")
	}
	return c.HTTPClient.QueryStream(ctx, logger, host, query, params)
}

func TestIntegrationVersionedCollapsingCopyPreservesRowsAcrossManyPartitions(test *testing.T) {
	fixture := newIntegrationDatabase(test)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	configureTestNodes(fixture, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		nodes[0][0].HTTP = &partitionLimitHTTPClient{HTTPClient: nodes[0][0].HTTP}
		return nodes
	})
	hostName := fixture.tablesClient.NodesByShard()[0][0].Host.Name
	connection := fixture.tablesClient.NodesByShard()[0][0].DB
	stagingTableName := "ready"
	stagingTable := fixture.tablesClient.QualifiedTableName(stagingTableName)
	assembly := ch_eo_common.NewTemporaryTable(fixture.table.TemporaryTablePrefix(), hostName, ch_eo_common.RoleAssembly)
	assemblyTable := fixture.tablesClient.QualifiedTableName(assembly.TableName)
	_, err := connection.ExecContext(ctx, "CREATE TABLE "+stagingTable+" (id UInt64, version UInt64, Sign Int8) ENGINE=MergeTree PARTITION BY id ORDER BY id")
	require.NoError(test, err)
	assemblyDDL, err := ch_eo_common.BuildCreateTableDDL(
		fixture.dstConfig.Database,
		assembly.TableName,
		assembly.ExpectedTableUUID,
		"CREATE TABLE t (id UInt64, version UInt64, Sign Int8) ENGINE=VersionedCollapsingMergeTree(Sign, version) PARTITION BY id ORDER BY id",
		"",
	)
	require.NoError(test, err)
	_, err = connection.ExecContext(ctx, assemblyDDL)
	require.NoError(test, err)
	_, err = connection.ExecContext(ctx, "INSERT INTO "+stagingTable+" SELECT number, number + 1, toInt8(1) FROM numbers(150) SETTINGS max_partitions_per_insert_block=0")
	require.NoError(test, err)

	require.NoError(test, commandsForTest(fixture).insertAllRowsFromTable(ctx, assembly, stagingTableName))
	rows, err := fixture.tablesClient.ReadStrings(ctx, hostName, "SELECT concat(toString(id), ':', toString(version), ':', toString(Sign)) FROM "+assemblyTable+" ORDER BY id")
	require.NoError(test, err)
	expectedRows := make([]string, 150)
	for rowIndex := range expectedRows {
		expectedRows[rowIndex] = fmt.Sprintf("%d:%d:1", rowIndex, rowIndex+1)
	}
	require.Equal(test, expectedRows, rows)
}
