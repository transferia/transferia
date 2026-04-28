package common

import (
	"fmt"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

type PositionType string

const (
	PositionSnapshotStarted = PositionType("Snapshot")
	PositionReplication     = PositionType("Replication")
)

type LogPosition struct {
	scn       uint64  // System change number
	rsID      *string // Record set ID, can be null
	ssn       *uint64 // SQL sequence number, can be null
	typ       PositionType
	timestamp time.Time
}

func NewLogPosition(scn uint64, rsID *string, ssn *uint64, typ PositionType, timestamp time.Time) (*LogPosition, error) {
	if !((IsNullString(rsID) && ssn == nil) || (!IsNullString(rsID) && ssn != nil)) {
		return nil, xerrors.Errorf("RSID and SSN must be nil, or have value together")
	}

	position := &LogPosition{
		scn:       scn,
		rsID:      nil,
		ssn:       nil,
		typ:       typ,
		timestamp: timestamp,
	}

	if IsNullString(rsID) {
		position.rsID = nil
	} else {
		inHouseRSID := *rsID
		position.rsID = &inHouseRSID
	}

	if ssn == nil {
		position.ssn = nil
	} else {
		inHoseSSN := *ssn
		position.ssn = &inHoseSSN
	}

	return position, nil
}

func (position *LogPosition) SCN() uint64 {
	return position.scn
}

func (position *LogPosition) RSID() *string {
	return position.rsID
}

func (position *LogPosition) SSN() *uint64 {
	return position.ssn
}

func (position *LogPosition) Timestamp() time.Time {
	return position.timestamp
}

func (position *LogPosition) Type() PositionType {
	return position.typ
}

func (position *LogPosition) OnlySCN() bool {
	return position.RSID() == nil && position.SSN() == nil
}

func (position *LogPosition) String() string {
	if position.OnlySCN() {
		return fmt.Sprintf("SCN: '%v', Time: '%v', Type: '%v'", position.SCN(), position.Timestamp(), position.Type())
	}

	return fmt.Sprintf("SCN: '%v', RSID: '%v', SSN: '%v', Type: '%v'", position.SCN(), position.RSID(), position.SSN(), position.Type())
}

func (position *LogPosition) ToOldLSN() (uint64, error) {
	return position.SCN(), nil
}

func (position *LogPosition) ToOldCommitTime() (uint64, error) {
	return uint64(position.timestamp.UnixNano()), nil
}
