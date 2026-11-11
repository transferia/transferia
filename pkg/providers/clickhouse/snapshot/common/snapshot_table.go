package ch_eo_common

import (
	"github.com/transferia/transferia/pkg/abstract"
)

const (
	PublicationNotDecided = ""
	PublicationDecided    = "decided"
	PublicationComplete   = "complete"
	PublicationDiscarded  = "discarded"
)

// SnapshotTable contains detached data for one source table. It has no storage
// client or persistence methods; the sink decides when changes become durable.
type SnapshotTable struct {
	TemporaryTablePrefix     string                 // Ownership prefix shared by this table's temporary data tables.
	StagingDDLSessionIDs     map[string]string      // Non-nil while staging is open; nil after all sessions have been fenced.
	PendingTableCommands     map[string]*OwnedTable `json:",omitempty"` // Unresolved table commands, keyed by table UUID; fence before discovery.
	SourceTableID            abstract.TableID
	DestinationTableName     string // final table name in clickhouse
	DDLTemplate              string
	UseReplicatedTableEngine bool
	NodeStates               []NodeState // publication state on each ClickHouse host: shard, previous UUID, candidate and progress
	PublicationStatus        string      // durable decision and completion of publication
}

// PublicationDecided includes completed publication, which cleanup must preserve.
func (s *SnapshotTable) PublicationDecided() bool {
	return s.PublicationStatus == PublicationDecided || s.PublicationStatus == PublicationComplete
}

func NewSnapshotTable(sourceTableID abstract.TableID, destinationTableName, ddlTemplate string, replicated bool) *SnapshotTable {
	return &SnapshotTable{
		TemporaryTablePrefix:     "",
		StagingDDLSessionIDs:     make(map[string]string),
		PendingTableCommands:     nil,
		SourceTableID:            sourceTableID,
		DestinationTableName:     destinationTableName,
		DDLTemplate:              ddlTemplate,
		UseReplicatedTableEngine: replicated,
		NodeStates:               nil,
		PublicationStatus:        "",
	}
}
