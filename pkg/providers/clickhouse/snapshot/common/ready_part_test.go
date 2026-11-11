package ch_eo_common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadyPartOwnsShardData(test *testing.T) {
	part := NewReadyPart()
	table := NewOwnedTable("host", "ready", "uuid")
	table.PendingCommand = NewCommand("session", "command")
	part.AddShard(*table, 42)
	table.TableName = "changed input"
	table.PendingCommand.SQL = "changed input command"
	part.AddShard(*NewOwnedTable("other-host", "other-ready", "other-uuid"), 7)
	require.Equal(test, 2, part.ShardCount())
	firstTable, firstRows := part.Shard(0)
	require.Equal(test, "ready", firstTable.TableName)
	require.Equal(test, "command", firstTable.PendingCommand.SQL)
	require.EqualValues(test, 42, firstRows)
	firstTable.PendingCommand.SQL = "changed output command"
	secondTable, secondRows := part.Shard(1)
	require.Equal(test, "other-ready", secondTable.TableName)
	require.EqualValues(test, 7, secondRows)
	restoredTable, restoredRows := part.Shard(0)
	require.Equal(test, "command", restoredTable.PendingCommand.SQL)
	require.EqualValues(test, 42, restoredRows)
}
