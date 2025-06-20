//go:build !disable_postgres_provider

package postgres

import (
	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util"
)

func CreateReplicationSlot(src *PgSource, tracker ...*Tracker) error {
	_, err := createReplicationSlot(src, true, tracker...)
	return err
}

// CreateReplicationSlotIfNotExists - returns 'true' if already exists
func CreateReplicationSlotIfNotExists(src *PgSource, tracker ...*Tracker) (bool, error) {
	return createReplicationSlot(src, false, tracker...)
}

func createReplicationSlot(src *PgSource, recreateIfExists bool, tracker ...*Tracker) (bool, error) {
	result, err := backoff.RetryNotifyWithData(func() (bool, error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Log.Warnf("Recovered from panic while creating a replication slot:\n%v", r)
			}
		}()
		conn, err := MakeConnPoolFromSrc(src, logger.Log)
		if err != nil {
			return false, xerrors.Errorf("failed to create a connection pool: %w", err)
		}
		defer conn.Close()
		slot, err := NewSlot(conn, logger.Log, src, tracker...)
		if err != nil {
			return false, xerrors.Errorf("failed to create a replication slot object: %w", err)
		}

		exist, err := slot.Exist()
		if err != nil {
			return false, xerrors.Errorf("failed to check existence of a replication slot: %w", err)
		}
		logger.Log.Infof("slot exists: %t", exist)
		if !recreateIfExists && exist {
			logger.Log.Infof("slot exists, recreateIfExists=false, won't recreate")
			return true, nil
		}
		if exist {
			logger.Log.Infof("replication slot already exists, try to drop it")
			if err := slot.Suicide(); err != nil {
				return false, xerrors.Errorf("failed to drop the replication slot: %w", err)
			}
			return false, xerrors.New("a replication slot already exists")
		}

		logger.Log.Info("will create slot")
		if err := slot.Create(); err != nil {
			return false, xerrors.Errorf("failed to create a replication slot: %w", err)
		}

		logger.Log.Info("Replication slot created, re-check existence")
		exist, err = slot.Exist()
		if err != nil {
			return false, xerrors.Errorf("failed to check existence of a replication slot: %w", err)
		}
		if !exist {
			return false, xerrors.New("replication slot was created, but the check shows it does not exist")
		}
		return false, nil
	}, backoff.WithMaxRetries(util.NewExponentialBackOff(), 3), util.BackoffLogger(logger.Log, "create replication slot"))
	if err != nil {
		return false, xerrors.Errorf("failed to create a replication slot: %w", err)
	}

	return result, nil
}
