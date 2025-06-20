//go:build !disable_postgres_provider

package postgres

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

var SelectLsnForSlot = `select restart_lsn from pg_replication_slots where slot_name = $1;`

type LsnTrackedSlot struct {
	logger    log.Logger
	once      sync.Once
	slotID    string
	childSlot AbstractSlot
	src       *PgSource
	schema    string
	Conn      *pgxpool.Pool
	lastMove  time.Time
	tracker   *Tracker
}

func (l *LsnTrackedSlot) Exist() (bool, error) {
	row := l.Conn.QueryRow(context.TODO(), fmt.Sprintf(`
select exists(
   select *
     from pg_replication_slots
   where slot_name = '%v' and (database is null or database = '%v')
);`, l.slotID, l.src.Database))

	var exist bool
	if err := row.Scan(&exist); err != nil {
		l.logger.Errorf("failed execute exists query: %v", err)
		return false, xerrors.Errorf("failed execute exists query: %w", err)
	}
	if !exist {
		state, err := l.tracker.GetLsn()
		if err != nil {
			if err == ErrNoKey {
				l.logger.Warnf("failed select lsn state from tracker: %v", err)
				return false, nil
			}
			return false, xerrors.Errorf("failed create slot from saved lsn: %w", err)
		}

		lsn := state.CommittedLsn
		if lsn == "" {
			return false, nil
		}
		if err := l.createFromLSN(lsn); err != nil {
			l.logger.Errorf("failed create slot from saved lsn: %v", err)
			return false, xerrors.Errorf("failed create slot from saved lsn: %w", err)
		}
		return true, nil
	}
	return exist, nil
}

func (l *LsnTrackedSlot) Create() error {
	rb := util.Rollbacks{}
	if err := l.childSlot.Create(); err != nil {
		return xerrors.Errorf("unable to create child slot: %w", err)
	}
	defer rb.Do()
	rb.Add(func() {
		if err := l.childSlot.Suicide(); err != nil {
			l.logger.Error("unable to kill child slot", log.Error(err))
		}
	})
	var lsn string
	if err := l.Conn.QueryRow(context.TODO(), SelectLsnForSlot, l.slotID).Scan(&lsn); err != nil {
		return err
	}
	l.logger.Infof("Inferred slot: %v | lsn: %v", l.slotID, lsn)
	if err := l.tracker.StoreLsn(l.slotID, lsn); err != nil {
		return xerrors.Errorf("unable to strore lsn: %w", err)
	}
	rb.Cancel()
	return nil
}

func (l *LsnTrackedSlot) Suicide() error {
	if err := l.tracker.RemoveLsn(); err != nil {
		l.logger.Warnf("failed cleanup lsn: %v", err)
	}
	return l.childSlot.Suicide()
}

func (l *LsnTrackedSlot) createFromLSN(lsn string) error {
	return backoff.Retry(func() error {
		rb := util.Rollbacks{}
		defer rb.Do()

		conn, err := MakeConnPoolFromSrc(l.src, l.logger)
		if err != nil {
			return xerrors.Errorf("could not create slot from lsn:%v because of error: %w", lsn, err)
		}
		rb.Add(func() {
			conn.Close()
		})

		l.Conn.Close()
		l.Conn = conn
		_, err = l.Conn.Exec(context.TODO(), fmt.Sprintf(`
BEGIN;
SET LOCAL lock_timeout = '0';
select * from pg_create_logical_replication_slot_lsn('%v', 'wal2json', false, pg_lsn('%v'));
COMMIT;
`, l.slotID, lsn))
		if err != nil {
			return xerrors.Errorf("could not create slot from lsn:%v because of error: %w", lsn, err)
		}
		l.logger.Infof("slot created from lsn:%v", lsn)
		rb.Cancel()
		return nil
	}, backoff.WithMaxRetries(util.NewExponentialBackOff(), 10))
}

func (l *LsnTrackedSlot) Move(lsn string) error {
	if time.Since(l.lastMove).Seconds() <= 1 {
		l.logger.Debugf("Skip move for lsn: %v, to fast moves: %v", lsn, time.Since(l.lastMove))
		return nil
	}
	if err := l.tracker.StoreLsn(l.slotID, lsn); err != nil {
		return err
	}
	l.lastMove = time.Now()
	return nil
}

func (l *LsnTrackedSlot) Close() {
	l.once.Do(func() {
		l.Conn.Close()
	})
}

func NewLsnTrackedSlot(pool *pgxpool.Pool, logger log.Logger, src *PgSource, tracker *Tracker) AbstractSlot {
	return &LsnTrackedSlot{
		logger:    logger,
		once:      sync.Once{},
		slotID:    src.SlotID,
		childSlot: NewNotTrackedSlot(pool, logger, src),
		src:       src,
		schema:    src.KeeperSchema,
		Conn:      pool,
		lastMove:  time.Time{},
		tracker:   tracker,
	}
}
