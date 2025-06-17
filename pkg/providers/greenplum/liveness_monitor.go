//go:build !disable_greenplum_provider

package greenplum

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

type livenessMonitor struct {
	coordinatorTx *gpTx

	closeCh chan struct{}
	closeWG sync.WaitGroup

	monitorCh chan error

	ctx context.Context
}

// newLivenessMonitor constructs a new monitor.
// The provided tx must not be accessed concurrently after it has been passed to this constructor up until the monitor is closed.
// The monitor will execute queries on the provided transaction periodically in a separate goroutine.
//
// Must be called with positive check interval.
func newLivenessMonitor(coordinatorTx *gpTx, ctx context.Context, checkInterval time.Duration) *livenessMonitor {
	result := &livenessMonitor{
		coordinatorTx: coordinatorTx,

		closeCh: make(chan struct{}),
		closeWG: sync.WaitGroup{},

		monitorCh: make(chan error, 1),

		ctx: ctx,
	}
	result.closeWG.Add(1)

	go result.run(checkInterval)

	return result
}

func (m *livenessMonitor) run(checkInterval time.Duration) {
	defer m.closeWG.Done()

	defer close(m.monitorCh)

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.closeCh:
			return
		case <-ticker.C:
			if err := m.checkLiveness(); err != nil {
				m.monitorCh <- xerrors.Errorf("liveness monitor detected an error: %w", err)
				return
			}
		}
	}
}

func (m *livenessMonitor) checkLiveness() error {
	err := m.coordinatorTx.withConnection(func(conn *pgx.Conn) error {
		_, err := conn.Exec(m.ctx, `SELECT 1`)
		return err
	})
	if err != nil {
		return xerrors.Errorf("liveness check failed: %w", err)
	}
	return nil
}

// Close waits until the monitor has actually been closed. The tx provided to the constructor can then be used safely.
func (m *livenessMonitor) Close() {
	if m == nil {
		return
	}

	close(m.closeCh)
	m.closeWG.Wait()
}

// C returns the monitoring channel of this monitor. The channel has the following properties:
//   - Just one object is sent into it until the channel is closed. After the sending, the channel is closed immediately;
//   - nil is sent when the monitor detects no errors during its whole lifetime. The sending happens when the monitor is closed;
//   - Non-nil error is sent when the monitor detects an error.
func (m *livenessMonitor) C() <-chan error {
	return m.monitorCh
}
