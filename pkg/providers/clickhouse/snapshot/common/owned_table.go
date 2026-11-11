package ch_eo_common

import "github.com/google/uuid"

// it's any temporary table, which we created in clickhouse:
//   - staging (_loading/_ready)
//   - assembly
//   - candidate
//
// OwnedTable describes a temporary ClickHouse table owned by the snapshot:
//   - part: rows of one source table part during upload;
//   - assembly: selected parts combined into one table;
//   - candidate: the prepared replacement, or the previous table after EXCHANGE.

type OwnedTable struct {
	CommandHostName   string   // Hostname used to execute commands for this table. In a Replicated database, part tables also exist on every other shard under the same name and UUID.
	TableName         string   // temporary table name on this host
	ExpectedTableUUID string   // expected system.tables.uuid, checked before modifying or dropping the table
	PendingCommand    *Command `json:",omitempty"` // Populated only in TableState.PendingTableCommands until the SQL outcome is resolved.
}

func NewOwnedTable(commandHostName, tableName, expectedTableUUID string) *OwnedTable {
	return &OwnedTable{
		CommandHostName:   commandHostName,
		TableName:         tableName,
		ExpectedTableUUID: expectedTableUUID,
		PendingCommand:    nil,
	}
}

// NewTemporaryTable describes a new table under the validated snapshot prefix.
// It does not create the table or persist state.
func NewTemporaryTable(prefix, commandHostName string, role tableRole) *OwnedTable {
	tableUUID := uuid.NewString()
	return NewOwnedTable(commandHostName, prefix+string(role)+"_"+tableUUID, tableUUID)
}
