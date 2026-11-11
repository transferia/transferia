package database_operations

import (
	"context"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
)

func TestReplicatedDatabaseWaitsInBoundedParallelism(test *testing.T) {
	sink := newDatabaseFixture(test)
	sink.databaseOperations = newReplicated(sink.logger, sink.transferID, sink.dstConfig.Database, sink.dstConfig.Cleanup, sink.tablesClient, sink.table.Snapshot(), testCheckpoint(sink.table))
	configureTestNodes(sink, func(_ [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		return nil
	},
	)
	started := make(chan string, 16)
	release := make(chan struct{})
	test.Cleanup(
		func() {
			select {
			case <-release:
			default:
				close(release)
			}
		},
	)
	for nodeIndex := 0; nodeIndex < 16; nodeIndex++ {
		host := "node-" + strconv.Itoa(nodeIndex)
		httpClient := newSessionHTTPClient(
			func(url.Values) error {
				started <- host
				<-release
				return nil
			},
		)
		database, mock, err := sqlmock.New()
		require.NoError(test, err)
		test.Cleanup(func() { _ = database.Close() })
		mock.ExpectQuery("SELECT toUInt8").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
		configureTestNodes(sink, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
			nodes = append(nodes, []ch_eo_common.Node{ch_eo_common.NewNode(&conn_clickhouse.Host{Name: host}, database, httpClient)})

			return nodes
		},
		)
	}
	done := make(chan error, 1)
	go func() { done <- sink.databaseOperations.WaitForDatabaseDDL(context.Background()) }()
	for startedCount := 0; startedCount < 8; startedCount++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			test.Fatal("database replica waits were serialized")
		}
	}
	select {
	case <-started:
		test.Fatal("database replica wait exceeded the concurrency limit")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		require.NoError(test, err)
	case <-time.After(time.Second):
		test.Fatal("database replica waits did not complete")
	}
	require.Len(test, started, 8)
}
