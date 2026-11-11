package database_operations

import (
	"context"
	"io"
	"net/url"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
)

func TestAtomicSnapshotAllowsUnavailableReplica(test *testing.T) {
	sink, primary := newDatabaseReadFixture(test)
	sink.databaseOperations = nil
	configureTestNodes(sink, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		nodes[0][0].HTTP = newSessionHTTPClient(func(url.Values) error { return nil })

		return nodes
	},
	)
	database, secondary, err := sqlmock.New()
	require.NoError(test, err)
	defer database.Close()

	configureTestNodes(sink, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		nodes[0] = append(nodes[0], ch_eo_common.NewNode(&conn_clickhouse.Host{Name: "secondary"}, database, nil))

		return nodes
	},
	)
	sink.tablesClient.SetReplicaFailureGrace(0)
	primary.ExpectQuery("SELECT toUInt8").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
	primary.ExpectQuery("SELECT engine FROM system.databases").WillReturnRows(sqlmock.NewRows([]string{"engine"}).AddRow("Atomic"))
	secondary.ExpectQuery("SELECT toUInt8").WillReturnError(io.ErrUnexpectedEOF)
	require.NoError(test, sink.initDatabaseOperations(context.Background()))
	require.False(test, sink.databaseOperations.(*atomicOperations).requireAllReplicas)
	require.NoError(test, secondary.ExpectationsWereMet())
}
