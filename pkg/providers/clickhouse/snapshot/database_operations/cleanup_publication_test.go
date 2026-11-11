package database_operations

import (
	"context"
	"io"
	"net/url"
	"testing"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
)

func TestCleanupKeepsUnpublishedCandidate(test *testing.T) {
	for _, publicationStatus := range []string{ch_eo_common.PublicationDecided, ch_eo_common.PublicationComplete} {
		test.Run(publicationStatus, func(test *testing.T) {
			for _, previousTableUUID := range []string{"", uuid.NewString()} {
				name := "new_destination"
				if previousTableUUID != "" {
					name = "existing_destination"
				}
				test.Run(name, func(test *testing.T) {
					fixture, mock := newDatabaseReadFixture(test)
					table := fixture.table.Snapshot()
					table.TemporaryTablePrefix = "__cleanup_"
					table.PublicationStatus = publicationStatus
					candidate := ch_eo_common.NewTemporaryTable(table.TemporaryTablePrefix, "unused", ch_eo_common.RoleCandidate)
					nodeState := ch_eo_common.NewCandidate("unused", 0, *candidate, 10)
					nodeState.PreviousTableUUID = previousTableUUID
					table.NodeStates = []ch_eo_common.NodeState{nodeState}
					httpRequests := 0
					configureTestNodes(fixture, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
						nodes[0][0].HTTP = newSessionHTTPClient(func(url.Values) error {
							httpRequests++
							return xerrors.New("cleanup must keep the unpublished candidate")
						})
						return nodes
					})
					checkpoints := 0
					operations := newAtomic(fixture.logger, fixture.transferID, fixture.dstConfig.Database, fixture.dstConfig.Cleanup, fixture.tablesClient, table, func(context.Context, string, *ch_eo_common.OwnedTable, *ch_eo_common.Command) error {
						checkpoints++
						return nil
					})
					mock.ExpectQuery("SELECT name FROM system.tables").WithArgs("test", table.TemporaryTablePrefix).
						WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow(candidate.TableName))
					mock.ExpectQuery("SELECT toString\\(uuid\\)").WithArgs("test", candidate.TableName).
						WillReturnRows(sqlmock.NewRows([]string{"uuid"}).AddRow(candidate.ExpectedTableUUID))
					destinationRows := sqlmock.NewRows([]string{"uuid"})
					if previousTableUUID != "" {
						destinationRows.AddRow(previousTableUUID)
					}
					mock.ExpectQuery("SELECT toString\\(uuid\\)").WithArgs("test", table.DestinationTableName).
						WillReturnRows(destinationRows)

					err := operations.dropTemporaryTablesOnNode(context.Background(), table, fixture.tablesClient.NodesByShard()[0][0])
					require.ErrorContains(test, err, "refusing to drop unpublished snapshot candidate unused/"+candidate.TableName)
					require.Zero(test, httpRequests)
					require.Zero(test, checkpoints)
					require.Equal(test, []ch_eo_common.NodeState{nodeState}, table.NodeStates)
					require.Empty(test, table.PendingTableCommands)
				})
			}
		})
	}
}

func TestCleanupDropsPublishedAndDiscardedTables(test *testing.T) {
	for _, scenario := range []string{"published_previous", "discarded_candidate", "undecided_candidate", "unrecorded_temporary_table"} {
		test.Run(scenario, func(test *testing.T) {
			fixture, mock := newDatabaseReadFixture(test)
			table := fixture.table.Snapshot()
			table.TemporaryTablePrefix = "__cleanup_"
			table.PublicationStatus = ch_eo_common.PublicationComplete
			candidate := ch_eo_common.NewTemporaryTable(table.TemporaryTablePrefix, "unused", ch_eo_common.RoleCandidate)
			nodeState := ch_eo_common.NewCandidate("unused", 0, *candidate, 10)
			nodeState.PreviousTableUUID = uuid.NewString()
			table.NodeStates = []ch_eo_common.NodeState{nodeState}
			temporaryTable := *candidate
			actualTableUUID := candidate.ExpectedTableUUID
			switch scenario {
			case "published_previous":
				actualTableUUID = nodeState.PreviousTableUUID
			case "discarded_candidate":
				table.PublicationStatus = ch_eo_common.PublicationDiscarded
			case "undecided_candidate":
				table.PublicationStatus = ch_eo_common.PublicationNotDecided
			case "unrecorded_temporary_table":
				temporaryTable = *ch_eo_common.NewTemporaryTable(table.TemporaryTablePrefix, "unused", ch_eo_common.RoleAssembly)
				actualTableUUID = temporaryTable.ExpectedTableUUID
			}
			dropRequests := 0
			configureTestNodes(fixture, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
				nodes[0][0].HTTP = newSessionHTTPClient(func(params url.Values) error {
					if params.Get("close_session") == "1" {
						return &clickhouse_go.Exception{Code: sessionNotFoundCode, Message: "Session not found"}
					}
					if params.Get("query_id") != "" {
						dropRequests++
					}
					return nil
				})
				return nodes
			})
			operations := newAtomic(fixture.logger, fixture.transferID, fixture.dstConfig.Database, fixture.dstConfig.Cleanup, fixture.tablesClient, table, testCheckpoint(fixture.table))
			mock.ExpectQuery("SELECT name FROM system.tables").WithArgs("test", table.TemporaryTablePrefix).
				WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow(temporaryTable.TableName))
			mock.ExpectQuery("SELECT toString\\(uuid\\)").WithArgs("test", temporaryTable.TableName).
				WillReturnRows(sqlmock.NewRows([]string{"uuid"}).AddRow(actualTableUUID))
			if scenario == "published_previous" {
				mock.ExpectQuery("SELECT toString\\(uuid\\)").WithArgs("test", table.DestinationTableName).
					WillReturnRows(sqlmock.NewRows([]string{"uuid"}).AddRow(candidate.ExpectedTableUUID))
			}
			mock.ExpectQuery("SELECT toString\\(uuid\\)").WithArgs("test", temporaryTable.TableName).
				WillReturnRows(sqlmock.NewRows([]string{"uuid"}).AddRow(actualTableUUID))
			mock.ExpectQuery("SELECT toString\\(uuid\\)").WithArgs("test", temporaryTable.TableName).
				WillReturnRows(sqlmock.NewRows([]string{"uuid"}))

			require.NoError(test, operations.dropTemporaryTablesOnNode(context.Background(), table, fixture.tablesClient.NodesByShard()[0][0]))
			require.Equal(test, 1, dropRequests)
			require.Empty(test, table.PendingTableCommands)
		})
	}
}

func TestAtomicCleanupKeepsCandidateWhenReplicaReturnsBetweenProbes(test *testing.T) {
	fixture, primary := newDatabaseReadFixture(test)
	database, secondary, err := sqlmock.New()
	require.NoError(test, err)
	test.Cleanup(func() {
		require.NoError(test, secondary.ExpectationsWereMet())
		_ = database.Close()
	})
	httpRequests := 0
	configureTestNodes(fixture, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		nodes[0] = append(nodes[0], ch_eo_common.NewNode(&conn_clickhouse.Host{Name: "returning"}, database, newSessionHTTPClient(func(url.Values) error {
			httpRequests++
			return xerrors.New("cleanup must keep the returning replica's candidate")
		})))
		return nodes
	})
	fixture.tablesClient.SetReplicaFailureGrace(0)
	table := fixture.table.Snapshot()
	table.TemporaryTablePrefix = "__cleanup_"
	table.PublicationStatus = ch_eo_common.PublicationComplete
	candidate := ch_eo_common.NewTemporaryTable(table.TemporaryTablePrefix, "returning", ch_eo_common.RoleCandidate)
	nodeState := ch_eo_common.NewCandidate("returning", 0, *candidate, 10)
	table.NodeStates = []ch_eo_common.NodeState{nodeState}
	checkpoints := 0
	operations := newAtomic(fixture.logger, fixture.transferID, fixture.dstConfig.Database, fixture.dstConfig.Cleanup, fixture.tablesClient, table, func(context.Context, string, *ch_eo_common.OwnedTable, *ch_eo_common.Command) error {
		checkpoints++
		return nil
	})
	primary.ExpectQuery("SELECT toUInt8").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
	secondary.ExpectQuery("SELECT toUInt8").WillReturnError(io.ErrUnexpectedEOF)
	primary.ExpectQuery("SELECT toUInt8").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
	secondary.ExpectQuery("SELECT toUInt8").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
	primary.ExpectQuery("SELECT name FROM system.tables").WithArgs("test", table.TemporaryTablePrefix).
		WillReturnRows(sqlmock.NewRows([]string{"name"}))
	secondary.ExpectQuery("SELECT name FROM system.tables").WithArgs("test", table.TemporaryTablePrefix).
		WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow(candidate.TableName))
	secondary.ExpectQuery("SELECT toString\\(uuid\\)").WithArgs("test", candidate.TableName).
		WillReturnRows(sqlmock.NewRows([]string{"uuid"}).AddRow(candidate.ExpectedTableUUID))
	secondary.ExpectQuery("SELECT toString\\(uuid\\)").WithArgs("test", table.DestinationTableName).
		WillReturnRows(sqlmock.NewRows([]string{"uuid"}))

	err = operations.DropTemporaryTables(context.Background())
	require.ErrorContains(test, err, "refusing to drop unpublished snapshot candidate returning/"+candidate.TableName)
	require.Zero(test, httpRequests)
	require.Zero(test, checkpoints)
	require.Equal(test, []ch_eo_common.NodeState{nodeState}, table.NodeStates)
	require.Empty(test, table.PendingTableCommands)
}
