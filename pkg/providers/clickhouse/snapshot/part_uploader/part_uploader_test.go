package part_uploader

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/sharding"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
)

func TestUploaderRejectsOtherTableParts(test *testing.T) {
	uploader := NewUploader(0, "test", false, nil, nil)
	test.Cleanup(func() { require.NoError(test, uploader.Close()) })
	rows := []abstract.ChangeItem{{Kind: abstract.InsertKind, Table: "first", PartID: "same-part"}}
	require.ErrorContains(test, uploader.Write(rows), "rows outside an open snapshot attempt")
	require.ErrorContains(test, uploader.Finish(context.Background(), rows[0].TablePartID()), "DoneTableLoad without an attempt")
	uploader.started = true
	uploader.tablePartID = rows[0].TablePartID()
	uploader.finished, uploader.reusesReadyPart = true, true
	require.NoError(test, uploader.Write(nil))
	require.NoError(test, uploader.Write(rows))
	require.NoError(test, uploader.Finish(context.Background(), uploader.tablePartID))
	for _, other := range []abstract.ChangeItem{
		{Kind: abstract.InsertKind, Table: "second", PartID: "same-part"},
		{Kind: abstract.InsertKind, Table: "first", PartID: "another-part"},
	} {
		require.ErrorContains(test, uploader.Write([]abstract.ChangeItem{rows[0], other}), "rows outside an open snapshot attempt")
		require.ErrorContains(test, uploader.Finish(context.Background(), other.TablePartID()), "DoneTableLoad without an attempt")
	}
	lostReply := xerrors.New("lost INSERT acknowledgement")
	uploader.poison = lostReply
	require.ErrorIs(test, uploader.Write(rows), lostReply)
	require.ErrorIs(test, uploader.Finish(context.Background(), uploader.tablePartID), lostReply)
}

func TestUploaderCompletionRetainsWritersAndPartMarker(test *testing.T) {
	for _, reusesReadyPart := range []bool{false, true} {
		test.Run(fmt.Sprintf("reused=%t", reusesReadyPart), func(test *testing.T) {
			uploader, rows, connections := newParallelUpload(test, nil)
			writers := uploader
			tablePartID := rows[0].TablePartID()
			uploader.finished = true
			uploader.reusesReadyPart = reusesReadyPart
			uploader.releaseCompletedUpload()

			table := ch_eo_common.NewSnapshotTable(tablePartID.TableID, "destination", "", false)
			// A completion marker must reject duplicate ownership before metadata I/O.
			require.ErrorContains(test, uploader.Start(context.Background(), table, tablePartID.PartID, nil, nil), "duplicate InitTableLoad")
			require.NoError(test, uploader.Finish(context.Background(), tablePartID))
			require.Equal(test, tablePartID, uploader.tablePartID)
			require.Nil(test, uploader.batches)
			require.Nil(test, uploader.shards)
			if reusesReadyPart {
				require.NoError(test, uploader.Write(rows[:1]))
			} else {
				require.ErrorContains(test, uploader.Write(rows[:1]), "rows outside an open snapshot attempt")
			}
			// Completion retains connection ownership until Close.
			require.NoError(test, writers.writeRows(0, "next_part", rows[:1]))
			require.NoError(test, writers.finishWrites())
			for _, connection := range connections {
				require.Zero(test, connection.closeCount)
			}
			require.NoError(test, uploader.Close())
			require.NoError(test, uploader.Close())
			for _, connection := range connections {
				require.Equal(test, 1, connection.closeCount)
			}
		})
	}
}

type failedStagingOperations struct {
	TableOperations
	err error
}

func (o *failedStagingOperations) WaitForStagingDDL(context.Context) error { return o.err }

func TestUploaderFailedStartDoesNotRegisterPart(test *testing.T) {
	table := ch_eo_common.NewSnapshotTable(abstract.TableID{Name: "source"}, "destination", "", false)
	client := ch_eo_common.NewClient(&clickhouse_model.ChDestination{Database: "test"}, logger.Log, [][]ch_eo_common.Node{{}})
	uploader := NewUploader(1, "test", false, nil, nil)
	writers := uploader
	test.Cleanup(func() { require.NoError(test, uploader.Close()) })
	require.ErrorContains(test, uploader.Start(context.Background(), table, "part", client, nil), "writer for shard 0 is missing")
	require.NoError(test, writers.AddShard(0, newUploadTestConnection(test), clickhouse_go.WithSettings(nil)))
	lostReply := xerrors.New("staging DDL acknowledgement failed")
	operations := &failedStagingOperations{TableOperations: nil, err: lostReply}
	require.ErrorIs(test, uploader.Start(context.Background(), table, "part", client, operations), lostReply)
	require.False(test, uploader.started)
}

func TestUploaderRejectsEventsAfterClose(test *testing.T) {
	uploader := NewUploader(0, "test", false, nil, nil)
	require.NoError(test, uploader.Close())
	require.NoError(test, uploader.Close())
	require.ErrorContains(test, uploader.Start(context.Background(), nil, "part", nil, nil), "closed")
	require.ErrorContains(test, uploader.Write(nil), "closed")
	require.ErrorContains(test, uploader.Finish(context.Background(), abstract.TablePartID{}), "closed")
}

func newParallelUpload(test *testing.T, write func(ch_eo_common.Node, string, []abstract.ChangeItem) error) (*PartUploader, []abstract.ChangeItem, []*uploadTestConnection) {
	test.Helper()
	nodes := make([][]ch_eo_common.Node, 32)
	rows := make([]abstract.ChangeItem, 32)
	connections := make([]*uploadTestConnection, 32)
	writers := NewUploader(32, "test", false, nil, nil)
	test.Cleanup(func() { require.NoError(test, writers.Close()) })
	schema := abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "id", DataType: "uint64", Required: true}})
	shards := make([]shard, 32)
	for shardIndex := range nodes {
		nodes[shardIndex] = []ch_eo_common.Node{ch_eo_common.NewNode(&conn_clickhouse.Host{Name: fmt.Sprint(shardIndex)}, new(sql.DB), nil)}
		shards[shardIndex] = newShard(*ch_eo_common.NewOwnedTable(fmt.Sprint(shardIndex), "stage", fmt.Sprint(shardIndex)), 0)
		rows[shardIndex] = abstract.ChangeItem{Kind: abstract.InsertKind, Table: "table", PartID: "part", ID: uint32(shardIndex), TableSchema: schema, ColumnNames: []string{"id"}, ColumnValues: []any{uint64(shardIndex)}}
		connections[shardIndex] = newUploadTestConnection(test)
		connections[shardIndex].batch.appendFunc = func(arguments []any) error {
			if write == nil {
				return nil
			}
			row := rows[shardIndex]
			row.ColumnValues = slices.Clone(arguments)
			return write(nodes[shardIndex][0], "stage", []abstract.ChangeItem{row})
		}
		require.NoError(test, writers.AddShard(shardIndex, connections[shardIndex], clickhouse_go.WithSettings(nil)))
	}
	cfg := &clickhouse_model.ChDestination{Database: "test", Cleanup: model.Drop}
	currentUpload := &PartUploader{started: true, tablePartID: rows[0].TablePartID(), partIDHash: "part", table: nil, client: ch_eo_common.NewClient(cfg, logger.Log, nodes), operations: nil, writers: writers.writers, sharder: func(item abstract.ChangeItem) sharding.ShardID { return sharding.ShardID(item.ID) }, batches: newShardBatches(32), shards: shards, reusesReadyPart: false, finished: false, poison: nil}
	return currentUpload, rows, connections
}

func TestDoneTableLoadWaitsForAllBufferedInsertsBeforeVerification(test *testing.T) {
	upload, rows, connections := newParallelUpload(test, nil)
	require.NoError(test, upload.Write(rows))
	started := make(chan struct{}, 32)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAll()
	finished := make(chan error, 1)
	lostReply := xerrors.New("lost final INSERT acknowledgement")
	for shardIndex, connection := range connections {
		connection.batch.sendFunc = func() error {
			started <- struct{}{}
			if shardIndex == 0 {
				return lostReply
			}
			<-release
			return nil
		}
	}
	go func() { finished <- upload.Finish(context.Background(), rows[0].TablePartID()) }()
	for shardIndex := 0; shardIndex < 32; shardIndex++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			releaseAll()
			test.Fatal("DoneTableLoad did not start all host acknowledgements")
		}
	}
	select {
	case <-finished:
		test.Fatal("DoneTableLoad returned while other hosts were still sending")
	default:
	}
	releaseAll()
	require.ErrorIs(test, <-finished, lostReply)
	require.False(test, upload.finished)
	require.ErrorIs(test, upload.Write(rows), lostReply)
	require.ErrorIs(test, upload.Finish(context.Background(), rows[0].TablePartID()), lostReply)
	// The fixture DB is intentionally uninitialized. Reaching the count query
	// despite the failed acknowledgement would panic instead of returning above.
}

func TestFinishWritesDoesNotTouchNonWriterReplicas(test *testing.T) {
	upload, rows, connections := newParallelUpload(test, nil)
	replica := upload.client.NodesByShard()[0][0]
	replica.Host = &conn_clickhouse.Host{Name: "non-writer"}
	configureUploadNodes(upload, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		nodes[0] = append(nodes[0], replica)

		return nodes
	},
	)
	require.NoError(test, upload.validateShards(len(upload.client.NodesByShard())))
	require.NoError(test, upload.Write(rows))
	require.NoError(test, upload.finishWrites())
	for _, connection := range connections {
		require.Equal(test, 1, connection.batch.sendCount)
	}
}

func TestPushWritesShardsConcurrentlyAndWaitsForAll(test *testing.T) {
	for _, fail := range []bool{false, true} {
		test.Run(fmt.Sprint(fail), func(test *testing.T) {
			started := make(chan string, 32)
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseAll()
			var calls atomic.Int32
			writeErr := xerrors.New("lost INSERT response")
			upload, rows, _ := newParallelUpload(
				test,
				func(node ch_eo_common.Node, _ string, _ []abstract.ChangeItem) error {
					calls.Add(1)
					started <- node.Host.Name
					if fail && node.Host.Name == "0" {
						return writeErr
					}
					<-release
					return nil
				},
			)
			done := make(chan error, 1)
			go func() { done <- upload.Write(rows) }()
			for shardIndex := 0; shardIndex < 32; shardIndex++ {
				select {
				case <-started:
				case <-time.After(5 * time.Second):
					releaseAll()
					test.Fatal("Push did not start every shard concurrently")
				}
			}
			select {
			case <-done:
				test.Fatal("Push returned while another shard was still writing")
			default:
			}
			releaseAll()
			err := <-done
			if fail {
				require.ErrorIs(test, err, writeErr)
				require.ErrorIs(test, upload.Write(rows), upload.poison)
				require.ErrorIs(test, upload.Finish(context.Background(), rows[0].TablePartID()), upload.poison)
			} else {
				require.NoError(test, err)
			}
			require.Equal(test, int32(32), calls.Load())
			for shardIndex, shard := range upload.shards {
				if fail && shardIndex == 0 {
					require.Zero(test, shard.expectedRows)
				} else {
					require.Equal(test, uint64(1), shard.expectedRows)
				}
			}
		})
	}
}

func TestPushDistinguishesFinishedAndReusedParts(test *testing.T) {
	for _, reused := range []bool{false, true} {
		test.Run(fmt.Sprintf("reused=%t", reused), func(test *testing.T) {
			var writes atomic.Int32
			upload, rows, _ := newParallelUpload(test, func(ch_eo_common.Node, string, []abstract.ChangeItem) error {
				writes.Add(1)
				return nil
			},
			)
			attempt := upload
			attempt.finished = true
			attempt.reusesReadyPart = reused
			pushError := upload.Write(rows)
			if reused {
				require.NoError(test, pushError)
			} else {
				require.ErrorContains(test, pushError, "rows outside an open snapshot attempt")
			}
			require.Zero(test, writes.Load())
			for _, shard := range attempt.shards {
				require.Zero(test, shard.expectedRows)
			}
		})
	}
}

func TestPushReplicaFailureDoesNotAcknowledgeShard(test *testing.T) {
	for _, lostReply := range []bool{false, true} {
		test.Run(fmt.Sprintf("lost_reply=%t", lostReply), func(test *testing.T) {
			var mutex sync.Mutex
			var calls atomic.Int32
			storedRowsByHost := make(map[string]int)
			writeError := xerrors.New("injected replica INSERT failure")
			upload, rows, _ := newParallelUpload(
				test,
				func(node ch_eo_common.Node, _ string, rows []abstract.ChangeItem) error {
					calls.Add(1)
					if node.Host.Name == "0" && !lostReply {
						return writeError
					}
					mutex.Lock()
					storedRowsByHost[node.Host.Name] += len(rows)
					mutex.Unlock()
					if node.Host.Name == "0" {
						return writeError
					}
					return nil
				},
			)
			attempt := upload
			sharedPartTable := attempt.shards[0].table
			for shardIndex := range upload.client.NodesByShard() {
				host := &conn_clickhouse.Host{Name: fmt.Sprintf("%d-secondary", shardIndex)}
				configureUploadNodes(upload, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
					nodes[shardIndex] = append(nodes[shardIndex], ch_eo_common.NewNode(host, new(sql.DB), nil))

					return nodes
				},
				)
				attempt.shards[shardIndex].table = sharedPartTable
			}
			err := upload.Write(rows)
			require.ErrorIs(test, err, writeError)
			require.Equal(test, int32(32), calls.Load())
			require.Zero(test, storedRowsByHost["0-secondary"])
			if lostReply {
				require.Equal(test, 1, storedRowsByHost["0"])
			} else {
				require.Zero(test, storedRowsByHost["0"])
			}
			require.Zero(test, attempt.shards[0].expectedRows)
			for _, shard := range attempt.shards[1:] {
				require.Equal(test, uint64(1), shard.expectedRows)
			}
			require.False(test, attempt.finished)
			// A middleware retry cannot duplicate the successful copies or turn
			// this partially acknowledged attempt into a ready part.
			require.ErrorIs(test, upload.Write(rows), upload.poison)
			require.ErrorIs(test, upload.Finish(context.Background(), rows[0].TablePartID()), upload.poison)
			require.Equal(test, int32(32), calls.Load())
		})
	}
}

func configureUploadNodes(currentUpload *PartUploader, configure func([][]ch_eo_common.Node) [][]ch_eo_common.Node) {
	currentUpload.client = ch_eo_common.NewClient(&clickhouse_model.ChDestination{Database: "test", Cleanup: model.Drop}, logger.Log, configure(currentUpload.client.NodesByShard()))
}

type uploadTestConnection struct {
	driver.Conn
	batch      *uploadTestBatch
	closeCount int
}

func newUploadTestConnection(test *testing.T) *uploadTestConnection {
	test.Helper()
	targetColumn, err := column.Type("UInt64").Column("id", &column.ServerContext{Timezone: time.UTC})
	require.NoError(test, err)
	return &uploadTestConnection{
		Conn:       nil,
		batch:      &uploadTestBatch{Batch: nil, columns: []column.Interface{targetColumn}},
		closeCount: 0,
	}
}

func (c *uploadTestConnection) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return c.batch, nil
}

func (c *uploadTestConnection) Close() error {
	c.closeCount++
	return nil
}

type uploadTestBatch struct {
	driver.Batch
	columns    []column.Interface
	appendFunc func([]any) error
	sendFunc   func() error
	sendCount  int
	abortCount int
	sent       bool
}

func (b *uploadTestBatch) Columns() []column.Interface { return slices.Clone(b.columns) }

func (b *uploadTestBatch) Append(arguments ...any) error {
	if b.appendFunc != nil {
		if err := b.appendFunc(arguments); err != nil {
			return err
		}
	}
	for columnIndex, argument := range arguments {
		if err := b.columns[columnIndex].AppendRow(argument); err != nil {
			return err
		}
	}
	return nil
}

func (b *uploadTestBatch) Send() error {
	b.sendCount++
	b.sent = true
	if b.sendFunc != nil {
		return b.sendFunc()
	}
	return nil
}

func (b *uploadTestBatch) Flush() error {
	for _, targetColumn := range b.columns {
		targetColumn.Reset()
	}
	return nil
}

func (b *uploadTestBatch) Abort() error {
	b.abortCount++
	b.sent = true
	return nil
}

func (b *uploadTestBatch) IsSent() bool { return b.sent }

func (b *uploadTestBatch) Close() error { return b.Abort() }

func TestUploaderWritersOwnOnlyRegisteredConnections(test *testing.T) {
	writers := NewUploader(2, "test", false, nil, nil)
	accepted := newSnapshotNativeTestConnection(nil)
	rejected := newSnapshotNativeTestConnection(nil)
	options := clickhouse_go.WithSettings(nil)
	require.ErrorContains(test, writers.AddShard(-1, rejected, options), "out of range")
	require.ErrorContains(test, writers.AddShard(2, rejected, options), "out of range")
	require.ErrorContains(test, writers.AddShard(0, nil, options), "missing")
	require.NoError(test, writers.AddShard(0, accepted, options))
	require.ErrorContains(test, writers.AddShard(0, rejected, options), "already registered")
	require.ErrorContains(test, writers.validateShards(1), "does not match")
	require.ErrorContains(test, writers.validateShards(2), "shard 1 is missing")
	require.ErrorContains(test, writers.writeRows(1, "table", nil), "shard 1 is missing")
	require.ErrorContains(test, writers.writeRows(2, "table", nil), "out of range")
	// Construction can fail before every shard is opened and before any part starts.
	require.NoError(test, writers.Close())
	require.NoError(test, writers.Close())
	require.Equal(test, 1, accepted.closeCount)
	require.Zero(test, rejected.closeCount)
	require.ErrorContains(test, writers.AddShard(1, rejected, options), "closed")
	require.ErrorContains(test, writers.writeRows(0, "table", nil), "closed")
	require.ErrorContains(test, writers.finishWrites(), "closed")
	require.ErrorContains(test, writers.validateShards(2), "closed")
}

func TestUploaderWritersFinishWaitsForEveryAcknowledgementAfterAnError(test *testing.T) {
	writers := NewUploader(32, "test", false, nil, nil)
	item := snapshotNativeStreamingTestItem()
	started := make(chan struct{}, 32)
	release := make(chan struct{})
	finished := make(chan error, 1)
	lostReply := xerrors.New("lost final INSERT acknowledgement")
	for shardIndex := 0; shardIndex < 32; shardIndex++ {
		batch := newSnapshotNativeTestBatch(snapshotNativeTestColumns(test, item.ColumnNames, []string{"String"}))
		batch.sendFunc = func() error {
			started <- struct{}{}
			if shardIndex == 0 {
				return lostReply
			}
			<-release
			return nil
		}
		connection := newSnapshotNativeTestConnection(func(context.Context, string) (driver.Batch, error) { return batch, nil })
		require.NoError(test, writers.AddShard(shardIndex, connection, clickhouse_go.WithSettings(nil)))
		require.NoError(test, writers.writeRows(shardIndex, "table", []abstract.ChangeItem{item}))
	}
	require.NoError(test, writers.validateShards(32))
	go func() { finished <- writers.finishWrites() }()
	for shardIndex := 0; shardIndex < 32; shardIndex++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			close(release)
			test.Fatal("Finish did not start every host acknowledgement")
		}
	}
	select {
	case <-finished:
		test.Fatal("Finish returned while other hosts were still sending")
	default:
	}
	close(release)
	require.ErrorIs(test, <-finished, lostReply)
	require.ErrorIs(test, writers.finishWrites(), lostReply)
	require.NoError(test, writers.Close())
}

func TestUploaderWritersReuseConnectionsAcrossCompletedParts(test *testing.T) {
	writers := NewUploader(1, "test", false, nil, nil)
	item := snapshotNativeStreamingTestItem()
	var batches []*snapshotNativeTestBatch
	connection := newSnapshotNativeTestConnection(func(context.Context, string) (driver.Batch, error) {
		batch := newSnapshotNativeTestBatch(snapshotNativeTestColumns(test, item.ColumnNames, []string{"String"}))
		batches = append(batches, batch)
		return batch, nil
	})
	require.NoError(test, writers.AddShard(0, connection, clickhouse_go.WithSettings(nil)))
	for _, tableName := range []string{"first_part_loading", "second_part_loading"} {
		require.NoError(test, writers.writeRows(0, tableName, []abstract.ChangeItem{item}))
		require.NoError(test, writers.finishWrites())
		require.Zero(test, connection.closeCount, "finishing a part must keep the shared connection open")
	}
	require.Len(test, batches, 2)
	for _, batch := range batches {
		require.Equal(test, 1, batch.sendCount)
		require.Len(test, batch.rows, 1)
		require.Zero(test, batch.abortCount)
	}
	require.NoError(test, writers.Close())
	require.Equal(test, 1, connection.closeCount)
}

func TestUploaderWritersCloseAbortsEveryWriterAfterAnError(test *testing.T) {
	writers := NewUploader(2, "test", false, nil, nil)
	item := snapshotNativeStreamingTestItem()
	connections := make([]*snapshotNativeTestConnection, 2)
	batches := make([]*snapshotNativeTestBatch, 2)
	firstCloseError := xerrors.New("first connection close failed")
	for shardIndex := range connections {
		batch := newSnapshotNativeTestBatch(snapshotNativeTestColumns(test, item.ColumnNames, []string{"String"}))
		connection := newSnapshotNativeTestConnection(func(context.Context, string) (driver.Batch, error) { return batch, nil })
		if shardIndex == 0 {
			connection.closeErr = firstCloseError
		}
		connections[shardIndex], batches[shardIndex] = connection, batch
		require.NoError(test, writers.AddShard(shardIndex, connection, clickhouse_go.WithSettings(nil)))
		require.NoError(test, writers.writeRows(shardIndex, "table", []abstract.ChangeItem{item}))
	}
	require.ErrorIs(test, writers.Close(), firstCloseError)
	require.NoError(test, writers.Close())
	for shardIndex, batch := range batches {
		require.Equal(test, 1, batch.abortCount)
		require.Zero(test, batch.sendCount, "shutdown must not confirm an unfinished part")
		require.Equal(test, 1, connections[shardIndex].closeCount)
	}
}
