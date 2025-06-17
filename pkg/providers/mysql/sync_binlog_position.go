//go:build !disable_mysql_provider

package mysql

import (
	"context"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
)

func GetLogFilePosition(storage *Storage) (string, uint64, string, error) {
	ctx := context.Background()
	tx, rollbacks, err := storage.getSnapshotQueryable(ctx)
	if err != nil {
		return "", 0, "", xerrors.Errorf("Can't get table read transaction: %w", err)
	}
	defer rollbacks.Do()

	file, pos, err := storage.getBinlogPosition(ctx, tx)
	if err != nil {
		return "", 0, "", xerrors.Errorf("unable to get binlog position: %w", err)
	}

	gtid, err := storage.getGtid(ctx, tx)
	if err != nil {
		return "", 0, "", xerrors.Errorf("unable to get gtid: %w", err)
	}

	logger.Log.Infof("Last binlog position: %v:%v", file, pos)
	return file, pos, gtid, nil
}

func SyncBinlogPosition(src *MysqlSource, id string, cp coordinator.Coordinator) error {
	storage, err := NewStorage(src.ToStorageParams())
	if err != nil {
		return xerrors.Errorf("failed to connect to the source database: %w", err)
	}
	defer func() { _ = storage.DB.Close() }()

	tracker, err := NewTracker(src, id, cp)
	if err != nil {
		return xerrors.Errorf("failed to create MySQL binlog tracker: %w", err)
	}

	logger.Log.Infof("sync binlog into %T", tracker)
	file, pos, gtid, err := GetLogFilePosition(storage)
	if err != nil {
		return xerrors.Errorf("failed to get log file position: %w", err)
	}

	flavor, _, err := CheckMySQLVersion(storage)
	if err != nil {
		return xerrors.Errorf("unable to check MySQL version: %w", err)
	}

	gtidModeEnabled, err := IsGtidModeEnabled(storage, flavor)
	if err != nil {
		return xerrors.Errorf("Unable to check gtid mode: %w", err)
	}

	if gtidModeEnabled {
		logger.Log.Infof("GTID mode is ON, trying to parse gtid: [%s]", gtid)

		gtidSet, err := mysql.ParseGTIDSet(flavor, gtid)
		if err != nil {
			return xerrors.Errorf("failed to parse gtidset: %w", err)
		}
		logger.Log.Infof("Store GTID: %v", gtidSet.String())
		if err := tracker.StoreGtidset(gtidSet); err != nil {
			return xerrors.Errorf("failed to save GTIDSet in the tracker: %w", err)
		}
	} else {
		if err := tracker.Store(file, pos); err != nil {
			return xerrors.Errorf("failed to save binlog position in the tracker: %w", err)
		}
	}

	return nil
}
