package logtracker

import (
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
)

type LogTracker interface {
	TransferID() string
	Init() error
	ClearPosition() error
	ReadPosition() (*oracle_common.LogPosition, error)
	WritePosition(position *oracle_common.LogPosition) error
}
