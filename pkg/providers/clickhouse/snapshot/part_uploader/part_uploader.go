package part_uploader

import (
	"context"
	"strings"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse/sharding"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
	"github.com/transferia/transferia/pkg/stats"
	"golang.org/x/sync/errgroup"
)

// PartUploader owns one source TablePartID's loading/ready lifecycle and streaming
// writers. Its caller serializes Start, Write, Finish and Close; writes to
// different shards run in parallel.
type PartUploader struct {
	tablePartID            abstract.TablePartID
	started                bool
	closed                 bool
	partIDHash             string
	table                  *ch_eo_common.SnapshotTable
	client                 *ch_eo_common.Client
	operations             TableOperations
	writers                []*shardWriter
	databaseName           string
	includeTransferColumns bool
	metrics                *stats.ChStats
	sharder                sharding.Sharder
	batches                *shardBatches
	shards                 []shard
	reusesReadyPart        bool
	finished               bool
	poison                 error
}

func NewUploader(shardCount int, databaseName string, includeTransferColumns bool, metrics *stats.ChStats, sharder sharding.Sharder) *PartUploader {
	return &PartUploader{
		tablePartID:            abstract.TablePartID{TableID: abstract.TableID{Namespace: "", Name: ""}, PartID: ""},
		started:                false,
		closed:                 false,
		partIDHash:             "",
		table:                  nil,
		client:                 nil,
		operations:             nil,
		writers:                make([]*shardWriter, shardCount),
		databaseName:           databaseName,
		includeTransferColumns: includeTransferColumns,
		metrics:                metrics,
		sharder:                sharder,
		batches:                nil,
		shards:                 nil,
		reusesReadyPart:        false,
		finished:               false,
		poison:                 nil,
	}
}

func (u *PartUploader) Start(ctx context.Context, table *ch_eo_common.SnapshotTable, partID string, client *ch_eo_common.Client, operations TableOperations) error {
	if u.closed {
		return xerrors.New("snapshot uploader is closed")
	}
	if u.started {
		return xerrors.New("duplicate InitTableLoad: uploader already owns a table part")
	}
	if err := u.validateShards(len(client.NodesByShard())); err != nil {
		return xerrors.Errorf("validate snapshot shard writers: %w", err)
	}
	if table.PublicationStatus != "" || table.StagingDDLSessionIDs == nil {
		return xerrors.New("snapshot table has already been closed")
	}
	if err := operations.WaitForStagingDDL(ctx); err != nil {
		return xerrors.Errorf("wait for staging DDL: %w", err)
	}
	partIDHash := ch_eo_common.Murmur128Hex(partID)
	attemptsByPart, err := discoverReadyAttempts(ctx, client, u.databaseName, table.TemporaryTablePrefix, table.TemporaryTablePrefix+partIDHash+"_")
	if err != nil {
		return xerrors.Errorf("discover ready part: %w", err)
	}
	var readyPart *ch_eo_common.ReadyPart
	if attempts := attemptsByPart[partIDHash]; len(attempts) != 0 {
		readyPart, err = restoreReady(ctx, client, operations, table, attempts[0], true)
		if err != nil {
			return xerrors.Errorf("read ready part: %w", err)
		}
	}
	var partTables []ch_eo_common.OwnedTable
	if readyPart == nil {
		partTables, err = operations.PrepareLoadingTables(ctx, partIDHash)
		if err != nil {
			return xerrors.Errorf("prepare loading tables: %w", err)
		}
	}
	u.tablePartID = abstract.TablePartID{TableID: table.SourceTableID, PartID: partID}
	u.started = true
	u.partIDHash = partIDHash
	u.table, u.client, u.operations = table, client, operations
	if readyPart != nil {
		u.reusesReadyPart, u.finished = true, true
		u.releaseCompletedUpload()
		return nil
	}
	u.batches = newShardBatches(len(client.NodesByShard()))
	for _, partTable := range partTables {
		u.shards = append(u.shards, newShard(partTable, 0))
	}
	return nil
}

// Write buffers rows only for the TablePartID accepted by Start.
func (u *PartUploader) Write(itemsForOneTablePart []abstract.ChangeItem) error {
	if u.closed {
		return xerrors.New("snapshot uploader is closed")
	}
	if len(itemsForOneTablePart) == 0 {
		return nil
	}
	for _, item := range itemsForOneTablePart {
		if !u.started || item.TablePartID() != u.tablePartID {
			return xerrors.New("rows outside an open snapshot attempt")
		}
	}
	if u.poison != nil {
		return xerrors.Errorf("upload is stopped: %w", u.poison)
	}
	if u.reusesReadyPart {
		return nil
	}
	if u.finished {
		return xerrors.New("rows outside an open snapshot attempt")
	}
	rowsByShard, err := u.batches.groupByShard(itemsForOneTablePart, u.sharder)
	defer u.batches.releaseRows()

	if err != nil {
		u.poison = xerrors.Errorf("write part: %w", err)
		return xerrors.Errorf("upload is stopped: %w", u.poison)
	}
	var writeGroup errgroup.Group
	writeGroup.SetLimit(32)
	for shardIndex, rows := range rowsByShard {
		if len(rows) == 0 {
			continue
		}
		writeGroup.Go(
			func() error {
				partTable := &u.shards[shardIndex].table
				if partTable.ExpectedTableUUID == "" {
					return xerrors.New("missing part table for snapshot load attempt")
				}
				// The upload host is fixed for the whole operation. Replicated staging
				// distributes physical parts; workers never fan out the same rows.
				writeErr := u.writeRows(shardIndex, partTable.TableName, rows)
				if writeErr != nil {
					return xerrors.Errorf("snapshot attempt %s must be discarded: %w", u.partIDHash, writeErr)
				}
				u.shards[shardIndex].expectedRows += uint64(len(rows))
				return nil
			},
		)
	}
	// Wait even after an error: no writes may outlive Push or race with Close,
	// verification, or another attempt. A failed attempt is never replayed.
	if err := writeGroup.Wait(); err != nil {
		u.poison = xerrors.Errorf("write part: %w", err)
		return xerrors.Errorf("upload is stopped: %w", u.poison)
	}
	return nil
}

func (u *PartUploader) Finish(ctx context.Context, tablePartID abstract.TablePartID) error {
	if u.closed {
		return xerrors.New("snapshot uploader is closed")
	}
	if !u.started || tablePartID != u.tablePartID {
		return xerrors.New("DoneTableLoad without an attempt")
	}
	if u.poison != nil {
		return xerrors.Errorf("upload is stopped: %w", u.poison)
	}
	if u.finished {
		return nil
	}
	if err := u.finish(ctx); err != nil {
		u.poison = err
		return xerrors.Errorf("finish upload: %w", err)
	}
	u.finished = true
	u.releaseCompletedUpload()
	return nil
}

func (u *PartUploader) finish(ctx context.Context) error {
	if err := u.finishWrites(); err != nil {
		return xerrors.Errorf("finish snapshot INSERTs before verification: %w", err)
	}
	nodesByShard := u.client.NodesByShard()
	var checks errgroup.Group
	checks.SetLimit(32)
	for shardIndex, currentShard := range u.shards {
		checks.Go(func() error {
			partTable := currentShard.table
			if err := u.client.CheckTableUUIDAndRowCount(ctx, nodesByShard[shardIndex][0].Host.Name, partTable.TableName, partTable.ExpectedTableUUID, currentShard.expectedRows); err != nil {
				return xerrors.Errorf("verify part: %w", err)
			}
			return nil
		})
	}
	// No ready marker may appear until every shard has been verified.
	if err := checks.Wait(); err != nil {
		return xerrors.Errorf("verify snapshot shards: %w", err)
	}
	return u.renameTablesToReady(ctx)
}

func (u *PartUploader) renameTablesToReady(ctx context.Context) error {
	renamedTableUUIDs := make(map[string]bool)
	var renames errgroup.Group
	renames.SetLimit(32)
	for shardIndex := range u.shards {
		partTable := u.shards[shardIndex].table
		readyTableName := strings.TrimSuffix(partTable.TableName, "_loading") + "_ready"
		if partTable.ExpectedTableUUID == "" {
			// All tasks already admitted must finish before the caller can clean up.
			_ = renames.Wait()
			return xerrors.New("part table disappeared before rename")
		}
		if renamedTableUUIDs[partTable.ExpectedTableUUID] {
			continue
		}
		renamedTableUUIDs[partTable.ExpectedTableUUID] = true
		renames.Go(func() error {
			if err := u.operations.RenamePartTable(ctx, &partTable, readyTableName); err != nil {
				return xerrors.Errorf("mark part ready: %w", err)
			}
			actualTableUUID, err := u.client.TableUUID(ctx, partTable.CommandHostName, readyTableName)
			if err != nil {
				return xerrors.Errorf("check ready rename: %w", err)
			}
			if actualTableUUID != partTable.ExpectedTableUUID {
				return xerrors.New("ready rename UUID mismatch")
			}
			return nil
		})
	}
	if err := renames.Wait(); err != nil {
		return xerrors.Errorf("rename snapshot shards: %w", err)
	}
	for shardIndex := range u.shards {
		partTable := &u.shards[shardIndex].table
		partTable.TableName = strings.TrimSuffix(partTable.TableName, "_loading") + "_ready"
	}
	return nil
}

// The uploader retains the completion marker until shutdown. No row buffers or
// per-part copies of the publication record need to stay alive after completion.
func (u *PartUploader) releaseCompletedUpload() {
	u.table = nil
	u.client = nil
	u.operations = nil
	u.batches = nil
	u.shards = nil
}

// Close aborts unconfirmed INSERTs and closes all shard writers, including
// writers opened before Start or before the sink finished initializing.
func (u *PartUploader) Close() error {
	if u.closed {
		return nil
	}
	u.closed = true
	err := u.closeWriters()
	u.writers = nil
	u.releaseCompletedUpload()
	u.sharder = nil
	if err != nil {
		return xerrors.Errorf("close snapshot writers: %w", err)
	}
	return nil
}

// AddShard takes ownership of the connection only on success.
func (u *PartUploader) AddShard(shardIndex int, connection driver.Conn, insertOptions clickhouse_go.QueryOption) error {
	if u.closed {
		return xerrors.New("snapshot native writers are closed")
	}
	if shardIndex < 0 || shardIndex >= len(u.writers) {
		return xerrors.Errorf("snapshot shard index %d is out of range", shardIndex)
	}
	if u.writers[shardIndex] != nil {
		return xerrors.Errorf("snapshot writer for shard %d is already registered", shardIndex)
	}
	if connection == nil {
		return xerrors.Errorf("snapshot connection for shard %d is missing", shardIndex)
	}
	u.writers[shardIndex] = newShardWriter(connection, u.databaseName, u.includeTransferColumns, insertOptions, u.metrics)
	return nil
}

func (u *PartUploader) validateShards(shardCount int) error {
	if u.closed {
		return xerrors.New("snapshot native writers are closed")
	}
	if len(u.writers) != shardCount {
		return xerrors.New("snapshot writer count does not match shard count")
	}
	for shardIndex, currentWriter := range u.writers {
		if currentWriter == nil {
			return xerrors.Errorf("snapshot writer for shard %d is missing", shardIndex)
		}
	}
	return nil
}

func (u *PartUploader) writeRows(shardIndex int, tableName string, items []abstract.ChangeItem) error {
	if u.closed {
		return xerrors.New("snapshot native writers are closed")
	}
	if shardIndex < 0 || shardIndex >= len(u.writers) {
		return xerrors.Errorf("snapshot shard index %d is out of range", shardIndex)
	}
	currentWriter := u.writers[shardIndex]
	if currentWriter == nil {
		return xerrors.Errorf("snapshot writer for shard %d is missing", shardIndex)
	}
	if err := currentWriter.Write(tableName, items); err != nil {
		return xerrors.Errorf("write snapshot rows on shard %d: %w", shardIndex, err)
	}
	return nil
}

// Finish confirms all pending INSERTs before a part's row counts are checked.
// Even after an error, every writer must finish before this method returns.
func (u *PartUploader) finishWrites() error {
	if u.closed {
		return xerrors.New("snapshot native writers are closed")
	}
	var finishGroup errgroup.Group
	finishGroup.SetLimit(32)
	for shardIndex, currentWriter := range u.writers {
		if currentWriter == nil {
			continue
		}
		finishGroup.Go(func() error {
			if err := currentWriter.Finish(); err != nil {
				return xerrors.Errorf("finish writes on shard %d: %w", shardIndex, err)
			}
			return nil
		})
	}
	if err := finishGroup.Wait(); err != nil {
		return xerrors.Errorf("finish writes: %w", err)
	}
	return nil
}

func (u *PartUploader) closeWriters() error {
	var firstCloseError error
	for shardIndex, currentWriter := range u.writers {
		if currentWriter == nil {
			continue
		}
		if err := currentWriter.Close(); err != nil {
			if firstCloseError == nil {
				firstCloseError = xerrors.Errorf("close snapshot writer on shard %d: %w", shardIndex, err)
			}
		}
	}
	u.writers = nil
	return firstCloseError
}
