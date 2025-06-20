//go:build !disable_mysql_provider

package mysql

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
)

const (
	gtidsetKey   = "gtidset"
	binlogPosKey = "binlog"
)

type Tracker struct {
	cp         coordinator.Coordinator
	transferID string
}

func (n *Tracker) StoreGtidset(gtidset mysql.GTIDSet) error {
	logger.Log.Infof("track gtidset %v", gtidset.String())
	return n.cp.SetTransferState(n.transferID, map[string]*coordinator.TransferStateData{
		gtidsetKey: {
			MysqlGtid: &coordinator.MysqlGtidState{
				Gtid:   gtidset.String(),
				Flavor: string(getFlavor(gtidset)),
			},
		},
	})
}

func (n *Tracker) RemoveGtidset() error {
	return n.cp.RemoveTransferState(n.transferID, []string{gtidsetKey})
}

func getFlavor(gtidset mysql.GTIDSet) MysqlFlavorType {
	switch gtidset.(type) {
	case *mysql.MysqlGTIDSet:
		return MysqlFlavorTypeMysql
	case *mysql.MariadbGTIDSet:
		return MysqlFlavorTypeMariaDB
	}
	return ""
}

func (n *Tracker) GetGtidset() (mysql.GTIDSet, error) {
	res, err := n.cp.GetTransferState(n.transferID)
	if err != nil {
		return nil, xerrors.Errorf("unable to get transfer state: %w", err)
	}
	value, ok := res[gtidsetKey]
	if !ok {
		return nil, xerrors.Errorf("state not contains %s key", gtidsetKey)
	}
	gtidsetV := value.GetMysqlGtid()
	if gtidsetV == nil {
		return nil, xerrors.Errorf("unexpected state type %v", value)
	}
	gtidset, err := mysql.ParseGTIDSet(gtidsetV.Flavor, gtidsetV.Gtid)
	if err != nil {
		return nil, xerrors.Errorf("unable to parse gridset: %w", err)
	}
	return gtidset, nil
}

func (n *Tracker) Close() error {
	return nil
}

func (n *Tracker) Store(file string, pos uint64) error {
	logger.Log.Infof("track %v:%v", file, pos)
	return n.cp.SetTransferState(n.transferID, map[string]*coordinator.TransferStateData{
		binlogPosKey: {
			MysqlBinlogPosition: &coordinator.MysqlBinlogPositionState{
				File:     file,
				Position: int64(pos),
			},
		},
	})
}

func (n *Tracker) Remove() error {
	return n.cp.RemoveTransferState(n.transferID, []string{binlogPosKey})
}

func (n *Tracker) Get() (file string, pos uint32, err error) {
	res, err := n.cp.GetTransferState(n.transferID)
	if err != nil {
		return "", 0, xerrors.Errorf("unable to get transfer state: %w", err)
	}
	value, ok := res[binlogPosKey]
	if !ok {
		return "", 0, xerrors.Errorf("state not contains %s key", binlogPosKey)
	}
	position := value.GetMysqlBinlogPosition()
	if position == nil {
		return "", 0, xerrors.Errorf("unexpected state type %v", value)
	}
	return position.File, uint32(position.Position), nil
}

func NewTracker(src *MysqlSource, transferID string, cp coordinator.Coordinator) (result *Tracker, err error) {
	connectionParams, err := NewConnectionParams(src.ToStorageParams())
	if err != nil {
		return nil, xerrors.Errorf("failed to create connection params: %w", err)
	}
	if src.TrackerDatabase != "" {
		connectionParams.Database = src.TrackerDatabase
	}
	return &Tracker{
		cp:         cp,
		transferID: transferID,
	}, nil
}
