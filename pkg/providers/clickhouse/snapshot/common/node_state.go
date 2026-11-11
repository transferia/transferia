package ch_eo_common

// NodeState describes a candidate and pending publication command on one host.
type NodeState struct {
	HostName                  string     // server name
	ShardIndex                int        // shard index of this server
	PreviousTableUUID         string     // 'uuid' of prev table; if not exists - empty
	CandidateTable            OwnedTable // prepared table, which should got new name
	ExpectedRowCount          uint64     // physical rows verified before candidate merges were resumed
	PendingPublicationCommand *Command   `json:",omitempty"` // 'EXCHANGE...'/'RENAME...'
}

func NewNodeState(hostName string, shardIndex int, previousTableUUID string) NodeState {
	return NodeState{
		HostName:                  hostName,
		ShardIndex:                shardIndex,
		PreviousTableUUID:         previousTableUUID,
		CandidateTable:            *NewOwnedTable("", "", ""),
		ExpectedRowCount:          0,
		PendingPublicationCommand: nil,
	}
}

// NewCandidate returns detached build data. The caller must verify the complete
// build before returning its results to the sink for persistence.
func NewCandidate(hostName string, shardIndex int, candidate OwnedTable, rows uint64) NodeState {
	node := NewNodeState(hostName, shardIndex, "")
	node.CandidateTable = *CopyOwnedTable(&candidate)
	node.ExpectedRowCount = rows
	return node
}
