package part_uploader

import (
	"context"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
)

type readyAttemptOperations struct {
	TableOperations
	rename func(*ch_eo_common.OwnedTable, string) error
}

func (o *readyAttemptOperations) PartTableIdentityHostName(hostName string) string {
	return hostName
}

func (o *readyAttemptOperations) RenamePartTable(_ context.Context, table *ch_eo_common.OwnedTable, readyName string) error {
	return o.rename(table, readyName)
}

func newReadyAttemptClient(test *testing.T) (*ch_eo_common.Client, []sqlmock.Sqlmock) {
	test.Helper()
	nodes := make([][]ch_eo_common.Node, 2)
	mocks := make([]sqlmock.Sqlmock, 2)
	for shardIndex := range nodes {
		database, mock, err := sqlmock.New()
		require.NoError(test, err)
		test.Cleanup(func() {
			require.NoError(test, mock.ExpectationsWereMet())
			_ = database.Close()
		})
		nodes[shardIndex] = []ch_eo_common.Node{ch_eo_common.NewNode(&conn_clickhouse.Host{Name: fmt.Sprintf("host%d", shardIndex)}, database, nil)}
		mocks[shardIndex] = mock
	}
	return ch_eo_common.NewClient(&clickhouse_model.ChDestination{Database: "test"}, logger.Log, nodes), mocks
}

func expectReadyAttemptUUID(mock sqlmock.Sqlmock, tableName string, tableUUID string) {
	rows := sqlmock.NewRows([]string{"uuid"})
	if tableUUID != "" {
		rows.AddRow(tableUUID)
	}
	mock.ExpectQuery("SELECT toString\\(uuid\\) FROM system.tables").WithArgs("test", tableName).WillReturnRows(rows)
}

func TestRestoreReadyDoesNotMixAttemptsAcrossShards(test *testing.T) {
	client, mocks := newReadyAttemptClient(test)
	table := ch_eo_common.NewSnapshotTable(abstract.TableID{Name: "source"}, "destination", "", false)
	table.TemporaryTablePrefix = "__ready_attempt_"
	partHash := ch_eo_common.Murmur128Hex("part")
	firstAttempt := partHash + "_" + ch_eo_common.Murmur128Hex("first")
	secondAttempt := partHash + "_" + ch_eo_common.Murmur128Hex("second")
	firstLoadingName := table.TemporaryTablePrefix + firstAttempt + "_loading"
	firstReadyName := table.TemporaryTablePrefix + firstAttempt + "_ready"
	secondLoadingName := table.TemporaryTablePrefix + secondAttempt + "_loading"
	var renamedTable *ch_eo_common.OwnedTable
	var renamedTo string
	operations := &readyAttemptOperations{
		TableOperations: nil,
		rename: func(currentTable *ch_eo_common.OwnedTable, readyName string) error {
			renamedTable = ch_eo_common.CopyOwnedTable(currentTable)
			renamedTo = readyName
			return nil
		},
	}
	for shardIndex, mock := range mocks {
		hostName := fmt.Sprintf("host%d", shardIndex)
		firstUUID := ch_eo_common.PartTableUUID(table.TemporaryTablePrefix, firstAttempt, hostName)
		if shardIndex == 0 {
			expectReadyAttemptUUID(mock, firstLoadingName, "")
			expectReadyAttemptUUID(mock, firstReadyName, firstUUID)
		} else {
			expectReadyAttemptUUID(mock, firstLoadingName, firstUUID)
			expectReadyAttemptUUID(mock, firstReadyName, "")
		}
		expectReadyAttemptUUID(mock, firstReadyName, firstUUID)
		mock.ExpectQuery("SELECT sum\\(rows\\) FROM system.parts").WithArgs("test", firstReadyName).
			WillReturnRows(sqlmock.NewRows([]string{"rows"}).AddRow(shardIndex + 2))
		expectReadyAttemptUUID(mock, secondLoadingName, ch_eo_common.PartTableUUID(table.TemporaryTablePrefix, secondAttempt, hostName))
	}

	readyPart, err := restoreReady(context.Background(), client, operations, table, firstAttempt, true)
	require.NoError(test, err)
	require.NotNil(test, readyPart)
	require.Equal(test, ch_eo_common.NewOwnedTable("host1", firstLoadingName, ch_eo_common.PartTableUUID(table.TemporaryTablePrefix, firstAttempt, "host1")), renamedTable)
	require.Equal(test, firstReadyName, renamedTo)
	for shardIndex := range mocks {
		hostName := fmt.Sprintf("host%d", shardIndex)
		readyTable, rowCount := readyPart.Shard(shardIndex)
		require.Equal(test, firstReadyName, readyTable.TableName)
		require.EqualValues(test, shardIndex+2, rowCount)
		// The newer attempt remains loading on both shards. A ready marker on
		// one shard of the older attempt must never certify either of them.
		secondUUID, err := client.TableUUID(context.Background(), hostName, secondLoadingName)
		require.NoError(test, err)
		require.Equal(test, ch_eo_common.PartTableUUID(table.TemporaryTablePrefix, secondAttempt, hostName), secondUUID)
	}
}

func TestDiscoverReadyPrefersCompleteAttemptAcrossShards(test *testing.T) {
	client, mocks := newReadyAttemptClient(test)
	table := ch_eo_common.NewSnapshotTable(abstract.TableID{Name: "source"}, "destination", "", false)
	table.TemporaryTablePrefix = "__ready_attempt_"
	partHash := ch_eo_common.Murmur128Hex("part")
	partialAttempt := partHash + "_00000000000000000000000000000000"
	completeAttempt := partHash + "_ffffffffffffffffffffffffffffffff"
	unverifiedAttempt := partHash + "_11111111111111111111111111111111"
	completeReadyName := table.TemporaryTablePrefix + completeAttempt + "_ready"
	operations := &readyAttemptOperations{
		TableOperations: nil,
		rename: func(*ch_eo_common.OwnedTable, string) error {
			return xerrors.New("discovery must use the already complete attempt")
		},
	}
	for shardIndex, mock := range mocks {
		partialSuffix := "_ready"
		if shardIndex == 1 {
			partialSuffix = "_loading"
		}
		mock.ExpectQuery("SELECT name FROM system.tables").WithArgs("test", table.TemporaryTablePrefix).
			WillReturnRows(sqlmock.NewRows([]string{"name"}).
				AddRow(table.TemporaryTablePrefix + partialAttempt + partialSuffix).
				AddRow(completeReadyName).
				AddRow(table.TemporaryTablePrefix + unverifiedAttempt + "_loading"))
		hostName := fmt.Sprintf("host%d", shardIndex)
		completeUUID := ch_eo_common.PartTableUUID(table.TemporaryTablePrefix, completeAttempt, hostName)
		expectReadyAttemptUUID(mock, table.TemporaryTablePrefix+completeAttempt+"_loading", "")
		expectReadyAttemptUUID(mock, completeReadyName, completeUUID)
		expectReadyAttemptUUID(mock, completeReadyName, completeUUID)
		mock.ExpectQuery("SELECT sum\\(rows\\) FROM system.parts").WithArgs("test", completeReadyName).
			WillReturnRows(sqlmock.NewRows([]string{"rows"}).AddRow(shardIndex + 2))
	}

	readyParts, err := DiscoverReady(context.Background(), client, operations, "test", table)
	require.NoError(test, err)
	require.Len(test, readyParts, 1)
	for shardIndex := range mocks {
		readyTable, rowCount := readyParts[0].Shard(shardIndex)
		require.Equal(test, completeReadyName, readyTable.TableName)
		require.EqualValues(test, shardIndex+2, rowCount)
	}
}
