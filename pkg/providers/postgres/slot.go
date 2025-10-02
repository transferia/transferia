package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/errors/coded"
	"github.com/transferia/transferia/pkg/errors/codes"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type AbstractSlot interface {
	Exist() (bool, error)
	Close()
	Create() error
	Suicide() error
}

type Slot struct {
	logger  log.Logger
	once    sync.Once
	slotID  string
	conn    *pgxpool.Pool
	src     *PgSource
	version PgVersion
}

func (slot *Slot) Exist() (bool, error) {
	var exist bool
	var exErr error

	err := backoff.Retry(func() error {
		exist, exErr = slot.exists(context.Background())
		return exErr
	}, backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), 4))

	return exist, err
}

func (slot *Slot) exists(ctx context.Context) (bool, error) {
	query := `
select exists(
   select *
     from pg_replication_slots
   where slot_name = $1 and (database is null or database = $2)
)`

	row := slot.conn.QueryRow(ctx, query, slot.slotID, slot.src.Database)

	var exists bool
	if err := row.Scan(&exists); err != nil {
		slot.logger.Errorf("got error on scan exists: %v", err)
		return false, xerrors.Errorf("slot check query failed: %w", err)
	}

	slot.logger.Infof("slot %s in database %s exist: %v", slot.slotID, slot.src.Database, exists)

	return exists, nil
}

func (slot *Slot) tx(operation txOp) error {
	return doUnderTransaction(context.Background(), slot.conn, operation, slot.logger)
}

func (slot *Slot) Create() error {
	return slot.tx(func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '0'"); err != nil {
			return xerrors.Errorf("failed to set lock_timeout: %w", err)
		}
		stmt, err := tx.Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'wal2json')", slot.slotID)
		slot.logger.Info("Create slot", log.Any("stmt", stmt))
		if err != nil {
			// Map "all replication slots are in use" (SQLSTATE 53400) to coded error
			if util.ContainsAnySubstrings(err.Error(), "SQLSTATE 53400", "all replication slots are in use") {
				return coded.Errorf(codes.PostgresReplicationSlotsInUse, "failed to create a replication slot: %w", err)
			}
			return xerrors.Errorf("failed to create a replication slot: %w", err)
		}
		return nil
	})
}

func (slot *Slot) Close() {
	slot.once.Do(func() {
		slot.conn.Close()
	})
}

func (slot *Slot) SuicideImpl() error {
	return slot.tx(func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '30s'"); err != nil {
			return xerrors.Errorf("failed to set lock_timeout: %w", err)
		}
		if _, err := tx.Exec(ctx, "select pg_terminate_backend(active_pid) from pg_replication_slots where slot_name = $1 and active_pid > 0", slot.slotID); err != nil {
			return xerrors.Errorf("failed to terminate active backends for slot: %w", err)
		}
		if _, err := tx.Exec(ctx, "select from pg_drop_replication_slot($1)", slot.slotID); err != nil {
			return xerrors.Errorf("failed to drop replication slot: %w", err)
		}

		slot.logger.Info("Drop slot query executed", log.String("slot_name", slot.slotID))
		return nil
	})
}

func (slot *Slot) Suicide() error {
	return backoff.Retry(
		func() error {
			exist, err := slot.Exist()
			if err != nil {
				msg := "unable to exit slot"
				slot.logger.Error(msg, log.Error(err))
				return xerrors.Errorf("%v: %w", msg, err)
			}

			if exist {
				slot.logger.Info("Will try to delete slot")
				err = slot.SuicideImpl()
				if err != nil {
					errMsg := fmt.Sprintf("slot.SuicideImpl() returned error: %s", err)
					slot.logger.Error(errMsg)
					return errors.New(errMsg)
				}
			} else {
				slot.logger.Info("Slot already deleted")
				return nil
			}

			slot.logger.Info("Slot should be deleted, double check")

			exist, err = slot.Exist()
			if err != nil {
				msg := "unable to exit slot"
				slot.logger.Error(msg, log.Error(err))
				return xerrors.Errorf("%v: %w", msg, err)
			}
			if exist {
				errMsg := "slot still exists after success deleting"
				slot.logger.Error(errMsg)
				return errors.New(errMsg)
			}
			return nil
		},
		backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), 4),
	)
}

func NewNotTrackedSlot(pool *pgxpool.Pool, logger log.Logger, src *PgSource) AbstractSlot {
	return &Slot{
		slotID:  src.SlotID,
		conn:    pool,
		logger:  logger,
		once:    sync.Once{},
		src:     src,
		version: ResolveVersion(pool),
	}
}

func NewSlot(pool *pgxpool.Pool, logger log.Logger, src *PgSource, tracker ...*Tracker) (AbstractSlot, error) {
	hasLSNTrack := false
	if err := pool.QueryRow(context.Background(), `
SELECT EXISTS (
        SELECT *
        FROM pg_catalog.pg_proc
        JOIN pg_namespace ON pg_catalog.pg_proc.pronamespace = pg_namespace.oid
        WHERE proname = 'pg_create_logical_replication_slot_lsn'
)`).Scan(&hasLSNTrack); err != nil {
		return nil, err
	}
	if hasLSNTrack {
		if len(tracker) == 0 {
			return nil, xerrors.Errorf("pg_tm_aux extension is present but no tracker was passed")
		}
		return NewLsnTrackedSlot(pool, logger, src, tracker[0]), nil
	}
	return NewNotTrackedSlot(pool, logger, src), nil
}
