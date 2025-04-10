package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dustin/go-humanize"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/format"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	slotByteLagQuery   = `select pg_current_wal_lsn() - restart_lsn as size from pg_replication_slots where slot_name = $1`
	slotByteLagQuery96 = `select pg_current_xlog_location() - restart_lsn as size from pg_replication_slots where slot_name = $1`
	slotExistsQuery    = `SELECT EXISTS(SELECT * FROM pg_replication_slots WHERE slot_name = $1 AND (database IS NULL OR database = $2))`
	peekFromSlotQuery  = `SELECT pg_logical_slot_peek_changes($1, NULL, 1)`

	healthCheckInterval = 5 * time.Second
)

type SlotMonitor struct {
	conn   pgxtype.Querier
	stopCh chan struct{}

	slotName         string
	slotDatabaseName string

	metrics *stats.SourceStats

	logger log.Logger
}

// NewSlotMonitor constructs a slot monitor, but does NOT start it.
//
// The provided pool is NOT closed by the monitor. It is the caller's responsibility to manage the pool.
func NewSlotMonitor(conn *pgxpool.Pool, slotName string, slotDatabaseName string, metrics *stats.SourceStats, logger log.Logger) *SlotMonitor {
	return &SlotMonitor{
		conn:   conn,
		stopCh: make(chan struct{}),

		slotName:         slotName,
		slotDatabaseName: slotDatabaseName,

		metrics: metrics,
		logger:  logger,
	}
}

func (m *SlotMonitor) Close() {
	if util.IsOpen(m.stopCh) {
		close(m.stopCh)
	}
}

func (m *SlotMonitor) describeError(err error) error {
	if err == pgx.ErrNoRows {
		return xerrors.Errorf("replication slot %s does not exist", m.slotName)
	}
	return err
}

func bytesToString(bytes int64) string {
	if bytes >= 0 {
		return humanize.Bytes(uint64(bytes))
	}
	return fmt.Sprintf("-%v", humanize.Bytes(uint64(-bytes)))
}

func (m *SlotMonitor) StartSlotMonitoring(maxSlotByteLag int64) <-chan error {
	result := make(chan error, 1)

	version := ResolveVersion(m.conn)

	go func() {
		defer close(result)
		ticker := time.NewTicker(healthCheckInterval)
		defer ticker.Stop()
		for {
			select {
			case <-m.stopCh:
				return
			case <-ticker.C:
				if err := m.checkSlot(version, maxSlotByteLag); err != nil {
					m.logger.Warn("check slot return error", log.Error(err))
					result <- err
					return
				}
			}
		}
	}()

	return result
}

func (m *SlotMonitor) checkSlot(version PgVersion, maxSlotByteLag int64) error {
	slotExists, err := m.slotExists(context.TODO())
	if err != nil {
		return xerrors.Errorf("unable to check existence of the slot %q: %w", m.slotName, err)
	}
	if !slotExists {
		return abstract.NewFatalError(xerrors.Errorf("slot %q has disappeared", m.slotName))
	}
	var slotByteLag int64
	var slotByteLagErr error
	if version.Is9x {
		slotByteLag, slotByteLagErr = m.getLag(slotByteLagQuery96)
	} else {
		slotByteLag, slotByteLagErr = m.getLag(slotByteLagQuery)
	}
	if slotByteLagErr != nil {
		return xerrors.Errorf("failed to check replication slot lag: %w", m.describeError(slotByteLagErr))
	}
	m.logger.Infof("replication slot %q WAL lag %s / %s", m.slotName, bytesToString(slotByteLag), format.SizeUInt64(uint64(maxSlotByteLag)))
	m.metrics.Usage.Set(float64(slotByteLag))
	if slotByteLag > maxSlotByteLag {
		return abstract.NewFatalError(xerrors.Errorf("byte lag for replication slot %q exceeds the limit: %d > %d", m.slotName, slotByteLag, maxSlotByteLag))
	}

	return nil
}

func (m *SlotMonitor) slotExists(ctx context.Context) (bool, error) {
	var result bool
	checkSlot := func() error {
		err := m.conn.QueryRow(ctx, slotExistsQuery, m.slotName, m.slotDatabaseName).Scan(&result)
		if err != nil {
			if !util.IsOpen(m.stopCh) {
				return nil
			}
			return xerrors.Errorf("failed to check slot existence: %w", err)
		}
		return nil
	}
	err := backoff.Retry(checkSlot, backoff.WithMaxRetries(util.NewExponentialBackOff(), 5))
	if err != nil {
		return false, err
	}
	return result, nil
}

func (m *SlotMonitor) validateSlot(ctx context.Context) error {
	// Was disabled: https://st.yandex-team.ru/TM-4783
	// Enabled for now https://st.yandex-team.ru/TM-7938, it is only used to check removed WAL segment
	validateSlot := func() error {
		rows, _ := m.conn.Query(withNotToLog(ctx), peekFromSlotQuery, m.slotName)
		for rows.Next() {
		}
		if err := rows.Err(); err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok && pgFatalCode[pgErr.Code] && strings.Contains(err.Error(), "has already been removed") {
				return abstract.NewFatalError(err)
			}
		}
		rows.Close()
		return nil
	}
	if err := backoff.Retry(validateSlot, backoff.WithMaxRetries(util.NewExponentialBackOff(), 5)); err != nil {
		return err
	}
	return nil

}

func (m *SlotMonitor) getLag(monitorQ string) (int64, error) {
	var slotByteLag int64
	getByteLag := func() error {
		err := m.conn.QueryRow(context.TODO(), monitorQ, m.slotName).Scan(&slotByteLag)
		if err != nil && !util.IsOpen(m.stopCh) {
			return nil
		}
		return err
	}
	err := backoff.Retry(getByteLag, backoff.WithMaxRetries(util.NewExponentialBackOff(), 5))
	return slotByteLag, err
}

type PostgresSlotKiller struct {
	Slot AbstractSlot
}

func (k *PostgresSlotKiller) KillSlot() error {
	return k.Slot.Suicide()
}

func RunSlotMonitor(ctx context.Context, pgSrc *PgSource, registry metrics.Registry, tracker ...*Tracker) (abstract.SlotKiller, <-chan error, error) {
	rb := util.Rollbacks{}
	defer rb.Do()

	connPool, err := MakeConnPoolFromSrc(pgSrc, logger.Log)
	if err != nil {
		return nil, nil, xerrors.Errorf("unable to make source connection pool: %w", err)
	}
	rb.Add(func() {
		connPool.Close()
	})

	slotMonitor := NewSlotMonitor(connPool, pgSrc.SlotID, pgSrc.Database, stats.NewSourceStats(registry), logger.Log)
	rb.Add(func() {
		slotMonitor.Close()
	})

	go func() {
		<-ctx.Done()
		slotMonitor.Close()
		logger.Log.Info("slot monitor is closed by context")
	}()

	exists, err := slotMonitor.slotExists(context.TODO())
	if err != nil {
		return nil, nil, xerrors.Errorf("unable to check existence of the slot: %w", err)
	}
	if !exists {
		return abstract.MakeStubSlotKiller(), nil, nil
	}

	errChan := slotMonitor.StartSlotMonitoring(int64(pgSrc.SlotByteLagLimit))

	pool, ok := slotMonitor.conn.(*pgxpool.Pool)
	if !ok {
		return nil, nil, xerrors.Errorf("unexpected type: %T", slotMonitor.conn)
	}
	slot, err := NewSlot(pool, logger.Log, pgSrc, tracker...)
	if err != nil {
		return nil, nil, xerrors.Errorf("unable to create new slot: %w", err)
	}

	rb.Cancel()
	return &PostgresSlotKiller{Slot: slot}, errChan, nil
}
