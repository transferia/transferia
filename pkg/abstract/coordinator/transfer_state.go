package coordinator

import (
	"time"

	"github.com/transferia/transferia/pkg/abstract"
)

type OraclePositionState struct {
	Scn       uint64
	RsID      string
	SSN       uint64
	Type      string
	Timestamp time.Time
}

type MysqlGtidState struct {
	Gtid   string
	Flavor string
}

type MysqlBinlogPositionState struct {
	File     string
	Position int64
}

type YtStaticPartState struct {
	SchemaName            string
	TableName             string
	PartID                string
	RotatedShardedTableID string
	YtTargetPath          string
	YtShardTargetPath     string
	YtShardTmpPath        string
}

// TransferStateData contain transfer state, shared across retries / restarts
// can contain any generic information about transfer progress
type TransferStateData struct {
	// Generic is recommended way, you can put anything json serializable here
	Generic any
	// IncrementalTables store current cursor progress for incremental tables
	IncrementalTables []abstract.TableDescription

	// Obsolete states, per-db, do not add new
	OraclePosition      *OraclePositionState
	MysqlGtid           *MysqlGtidState
	MysqlBinlogPosition *MysqlBinlogPositionState
	YtStaticPart        *YtStaticPartState
}

func (s *TransferStateData) GetMysqlBinlogPosition() *MysqlBinlogPositionState {
	if s == nil {
		return nil
	}
	return s.MysqlBinlogPosition
}

func (s *TransferStateData) GetMysqlGtid() *MysqlGtidState {
	if s == nil {
		return nil
	}
	return s.MysqlGtid
}

func (s *TransferStateData) GetOraclePosition() *OraclePositionState {
	if s == nil {
		return nil
	}
	return s.OraclePosition
}

func (s *TransferStateData) GetGeneric() any {
	if s == nil {
		return nil
	}
	return s.Generic
}

func (s *TransferStateData) GetIncrementalTables() []abstract.IncrementalState {
	if s == nil {
		return nil
	}
	return abstract.TableDescriptionToIncrementalState(s.IncrementalTables)
}
