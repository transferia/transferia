package ch_eo_common

// Command records SQL and its single-use HTTP session.
// Main saves this Command inside TableState before sending SQL.
// A session ID is never reinitialized after a lost creation response.
// Secondary staging CREATE, DROP and RENAME use separate shared sessions
// recorded in TableState.StagingDDLSessionIDs.
//
// Main executes these commands through this record:
//   - SELECT 1 FORMAT Null (session capability probe)
//   - CREATE TABLE
//   - ALTER TABLE ... ATTACH PARTITION
//   - ALTER TABLE ... REPLACE PARTITION
//   - EXCHANGE TABLES
//   - RENAME TABLE
//   - DROP TABLE ... SYNC

type Command struct {
	SessionID             string // ID of a single-use ClickHouse HTTP session; also used as 'query_id' to check execution progress & result
	SQL                   string // SQL executing Command
	ReplicaFetchTableName string // optional table name used to observe replica-fetch progress
}

// NewCommand creates a Command record for a prepared HTTP session.
// The constructor stores the SQL as supplied; it does not validate or execute it.
func NewCommand(sessionID, sqlStatement string) *Command {
	return &Command{
		SessionID:             sessionID,
		SQL:                   sqlStatement,
		ReplicaFetchTableName: "",
	}
}
