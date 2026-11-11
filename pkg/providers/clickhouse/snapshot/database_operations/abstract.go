// Package database_operations implements the Atomic and Replicated snapshot algorithms.
package database_operations

import (
	"context"
	"net/url"

	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
)

// DatabaseOperations exposes complete snapshot operations. ForTable returns an
// independent instance bound to detached table data and a synchronous command checkpoint.
type DatabaseOperations interface {
	// Returns 'DatabaseOperations' implementation for table (depends on database type)
	ForTable(*ch_eo_common.SnapshotTable, CommandCheckpoint) DatabaseOperations

	// Builds 'candidate'
	// 'candidate' - it's table, which will be EXCHANGEd with target table!
	// For 'Atomic'
	//     - On one node of every shard - build one MergeTree table, by one-by-one part ATTACH - it becames "assembly" table
	//     - On every available node of every shard - create "candidate" table - "CREATE TABLE ..." (without 'ON CLUSTER')
	//     - On one node of every shard - exec "REPLACE PARTITION ... FROM assembly": "assembly" copied to "candidate'
	//     - On every replica exec "SYSTEM SYNC REPLICA db.candidate_table"
	//     - Check 'UUID' and #rows_count of every replica
	// For 'Replicated'
	//     - Create "assembly" table - MergeTree/ReplacingMergeTree
	//     - If 'cleanup_policy'='disabled' - ATTACH target table to "assembly"
	//     - ONE TIME PER CLUSTER - one-by-one part ATTACH PARTITION - it becames "assembly" table
	//     - check #rows_count of "assembly"
	//     - Create "candidate" table - "CREATE TABLE ..." (without 'ON CLUSTER')
	//     - ONE TIME FOR EVERY PARTITION_ID PER CLUSTER - exec ATTACH: "assembly"->"candidate'
	//     - check candidate UUID and #rows_count on available replicas of every shard
	BuildCandidates(context.Context, []ch_eo_common.ReadyPart) ([]ch_eo_common.NodeState, error)

	// If 'target' exists - do 'EXCHANGE TABLES', else do 'RENAME TABLE'
	// For 'Atomic' - do it on available nodes (or all nodes in strict mode)
	// For 'Replicated' - do it once per cluster
	Publish(context.Context) error

	// After 'EXCHANGE' removed old table
	// For 'Atomic' - do it on available nodes (or all nodes in strict mode)
	// For 'Replicated' - do it once per cluster
	DropPreviousTables(context.Context) error

	// Prepares evenrthing for 'Cleanup' - wait finishing running commands, checks UUID
	// For 'Atomic' - checks all replicas
	// For 'Atomic' - check available replicas
	PreparePreviousPublicationForCleanup(context.Context) error

	// Waits finishing of running queries in DDL-sessions & closes them
	CloseStaging(context.Context) error

	// Waits finishing of running commands (for staging-tables) and closes their SQL-sessions
	// (it's PrepareSessionCommand(
	WaitForPendingCommandsAndClearRecords(context.Context) error

	// Finds tables by temporary operation prefix, checks their UUID & make 'DROP TABLE ... SYNC'
	DropTemporaryTables(context.Context) error

	// Temporary-table operations required by the uploader's local interface.
	WaitForStagingDDL(context.Context) error

	// Creates empty '_loading'-tables for one TablePartID
	// For 'Atomic' - creates MergeTree on choosed host of every shard
	// For 'Replicated' - one time creates 'ReplicatedMergeTree'
	PrepareLoadingTables(context.Context, string) ([]ch_eo_common.OwnedTable, error)

	// _loading -> _ready
	RenamePartTable(context.Context, *ch_eo_common.OwnedTable, string) error

	// Chooses hostname, used for staging tables & UUID calculations
	PartTableIdentityHostName(string) string

	// Returns nodes grouped by shard
	// For 'Atomic' - returns available nodes, or all configured nodes in strict mode
	// For 'Replicated' - returns all available nodex
	SelectNodesByShard(context.Context) ([][]ch_eo_common.Node, error)

	// Waits when available nodes finished table changes
	WaitForDatabaseDDL(context.Context) error

	// Checks whether the table has an acceptable number of replicas.
	IsReplicaCountCompatibleWithTopology(actual, configured, available int) bool

	// Returns missing nodes, including unavailable ones
	// For 'Atomic' - do nothing, in this mode should be queried all nodes
	// For 'Replicated' - adds all absent nodes from consiguration, includes unavailable
	MissingNodeStates(*ch_eo_common.SnapshotTable) []ch_eo_common.NodeState

	// Publishes any returned replicas before cleanup can remove their candidates.
	EnsurePublicationBeforeCleanup(context.Context) error

	//---

	// Choose host for command execution
	// For 'Atomic' - returns arg
	// For 'Replicated' - host ignored, returned host #0
	resolveCommandHostName(context.Context, string) (string, error)

	// Waits when node apply DDL-commands
	// For 'Atomic' - do nothing
	// For 'Replicated' - exec 'SYSTEM SYNC DATABASE REPLICA db'
	SyncDatabaseDDLOnNode(context.Context, string) error

	// For 'Atomic' - set replication_alter_partitions_sync=1 (2 in strict mode)
	// For 'Replicated' - turn-off waiting of distributed DDL
	configureQueryParams(string, url.Values)

	// For 'Atomic' - MergeTree()
	// For 'Replicated' - ReplicatedMergeTree(...)
	newStagingEngineSQL() string
}

// CommandCheckpoint hands command intent or resolution to the caller. A non-nil
// table identifies a private resource; nil identifies publication on hostName.
// A nil command means the previously recorded command has been fenced and checked.
// Returning an error prevents SQL execution and retries in this operation instance.
type CommandCheckpoint func(ctx context.Context, hostName string, table *ch_eo_common.OwnedTable, command *ch_eo_common.Command) error
