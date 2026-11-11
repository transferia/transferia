package database_operations

import (
	"context"
	"io"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
	"github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/part_uploader"
	"go.ytsaurus.tech/library/go/core/log"
)

// delayedStagingRename keeps the exact request that an earlier worker submitted
// before losing its connection. The test admits it only after reassignment.
type delayedStagingRename struct {
	httpclient.HTTPClient
	statement string
	params    url.Values
}

func (c *delayedStagingRename) QueryStream(ctx context.Context, logger log.Logger, host *conn_clickhouse.Host, query any, params url.Values) (io.ReadCloser, error) {
	if statement, ok := query.(string); ok && strings.HasPrefix(statement, "RENAME TABLE ") && c.statement == "" {
		c.statement = statement
		c.params = make(url.Values, len(params))
		for key, values := range params {
			c.params[key] = append([]string(nil), values...)
		}
		return nil, io.EOF
	}
	return c.HTTPClient.QueryStream(ctx, logger, host, query, params)
}

func TestIntegrationLateStagingRenameCannotCompleteReplacementAttempt(test *testing.T) {
	fixture := newIntegrationDatabase(test)
	ctx := context.Background()
	hostName := "127.0.0.1"
	session, err := fixture.tablesClient.PrepareSessionCommand(ctx, hostName, "SELECT 1 FORMAT Null")
	require.NoError(test, err)
	test.Cleanup(func() { require.NoError(test, fixture.tablesClient.FenceSession(ctx, hostName, session.SessionID)) })

	interruptedRename := &delayedStagingRename{
		HTTPClient: fixture.tablesClient.NodesByShard()[0][0].HTTP,
		statement:  "",
		params:     nil,
	}
	configureTestNodes(fixture, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		nodes[0][0].HTTP = interruptedRename
		return nodes
	})
	table := fixture.table.Snapshot()
	table.DDLTemplate = "CREATE TABLE source (id UInt64) ENGINE = MergeTree ORDER BY id"
	table.StagingDDLSessionIDs = map[string]string{hostName: session.SessionID}
	operations := fixture.databaseOperations.ForTable(table, testCheckpoint(fixture.table))
	partIDHash := ch_eo_common.Murmur128Hex("retried-part")
	connection := fixture.tablesClient.NodesByShard()[0][0].DB

	firstAttempt, err := operations.PrepareLoadingTables(ctx, partIDHash)
	require.NoError(test, err)
	require.Len(test, firstAttempt, 1)
	firstReadyName := strings.TrimSuffix(firstAttempt[0].TableName, "_loading") + "_ready"
	_, err = connection.Exec("INSERT INTO " + fixture.tablesClient.QualifiedTableName(firstAttempt[0].TableName) + " VALUES (1), (2)")
	require.NoError(test, err)
	require.NoError(test, fixture.tablesClient.CheckTableUUIDAndRowCount(ctx, hostName, firstAttempt[0].TableName, firstAttempt[0].ExpectedTableUUID, 2))
	require.ErrorIs(test, operations.RenamePartTable(ctx, &firstAttempt[0], firstReadyName), io.EOF)

	// The old worker has stopped. Its already submitted HTTP request has not
	// reached the session yet, so the new worker's barrier can overtake it.
	require.NoError(test, operations.WaitForStagingDDL(ctx))
	secondAttempt, err := operations.PrepareLoadingTables(ctx, partIDHash)
	require.NoError(test, err)
	require.Len(test, secondAttempt, 1)
	secondReadyName := strings.TrimSuffix(secondAttempt[0].TableName, "_loading") + "_ready"

	err = fixture.tablesClient.ExecuteCommandOverHTTP(ctx, hostName, interruptedRename.statement, interruptedRename.params)
	require.NoError(test, err)
	// A late request may finish A, whose rows were already verified. It must
	// not rename B or give B's empty table a ready marker.
	require.NoError(test, fixture.tablesClient.CheckTableUUIDAndRowCount(ctx, hostName, secondAttempt[0].TableName, secondAttempt[0].ExpectedTableUUID, 0))
	readyUUID, err := fixture.tablesClient.TableUUID(ctx, hostName, secondReadyName)
	require.NoError(test, err)
	require.Empty(test, readyUUID)
	require.NoError(test, fixture.tablesClient.CheckTableUUIDAndRowCount(ctx, hostName, firstReadyName, firstAttempt[0].ExpectedTableUUID, 2))

	_, err = connection.Exec("INSERT INTO " + fixture.tablesClient.QualifiedTableName(secondAttempt[0].TableName) + " VALUES (1), (2)")
	require.NoError(test, err)
	require.NoError(test, fixture.tablesClient.CheckTableUUIDAndRowCount(ctx, hostName, secondAttempt[0].TableName, secondAttempt[0].ExpectedTableUUID, 2))
	require.NoError(test, operations.RenamePartTable(ctx, &secondAttempt[0], secondReadyName))
	readyParts, err := part_uploader.DiscoverReady(ctx, fixture.tablesClient, operations, fixture.dstConfig.Database, table)
	require.NoError(test, err)
	require.Len(test, readyParts, 1)
	readyTable, readyRows := readyParts[0].Shard(0)
	require.Contains(test, []string{firstReadyName, secondReadyName}, readyTable.TableName)
	require.EqualValues(test, 2, readyRows)
}
