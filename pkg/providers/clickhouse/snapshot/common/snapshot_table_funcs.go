package ch_eo_common

func CopyOwnedTable(ownedTable *OwnedTable) *OwnedTable {
	if ownedTable == nil {
		return nil
	}
	result := *ownedTable
	result.PendingCommand = CopyCommand(ownedTable.PendingCommand)
	return &result
}

func CopyCommand(command *Command) *Command {
	if command == nil {
		return nil
	}
	result := *command
	return &result
}
