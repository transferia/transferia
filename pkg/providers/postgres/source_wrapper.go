//go:build !disable_postgres_provider

package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type Worker struct {
	status        chan error
	src           *PgSource
	transferID    string
	logger        log.Logger
	metrics       *stats.SourceStats
	tableLsns     map[string]uint64
	publisher     abstract.Source
	keeper        *Keeper
	slot          AbstractSlot
	conn          *pgxpool.Pool
	cp            coordinator.Coordinator
	objects       *model.DataObjects
	dbLogSnapshot bool
}

func (w *Worker) Run(sink abstract.AsyncSink) error {
	if w.src.IsHomo {
		if err := w.keeper.Init(sink); err != nil {
			//nolint:descriptiveerrors
			return err
		}
	}
	err := w.run(sink)
	w.logger.Error("Run done", log.Error(err))
	if err != nil {
		if abstract.IsFatal(err) {
			if err := w.slot.Suicide(); err != nil {
				w.logger.Errorf("slotID.Suicide() returned error: %s", err.Error())
			}
		}
	}
	//nolint:descriptiveerrors
	return err
}

func (w *Worker) run(sink abstract.AsyncSink) error {
	var lockCh chan bool
	for {
		l, err := w.keeper.Lock(w.src.SlotID)
		if err != nil {
			if abstract.IsFatal(err) {
				return xerrors.Errorf("failed to acquire consumer keeper lock: %w", err)
			}
			w.logger.Warn("Lock is not acquired. Sleep for 2 seconds", log.Error(err))
			w.metrics.Master.Set(0)
			time.Sleep(2 * time.Second)
			continue
		}

		lockCh = l
		break
	}

	pubStream, err := newWalSource(
		w.src,
		w.objects,
		w.transferID,
		w.metrics,
		w.logger,
		w.slot,
		w.cp,
		w.dbLogSnapshot,
	)
	if err != nil {
		//nolint:descriptiveerrors
		return err
	}
	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		errCh <- pubStream.Run(sink)
	}()

	w.publisher = pubStream
	defer w.metrics.Master.Set(0)
	defer sink.Close()

	select {
	case err := <-errCh:
		if err != nil {
			w.logger.Error("stream closed", log.Error(err))
		}
		//nolint:descriptiveerrors
		return err
	case <-w.keeper.CloseSign:
	case <-lockCh:
	}

	// We check errCh again in case an error has arrived at the same time as other signals, wait 1 second to make sure there is no error
	select {
	case err := <-errCh:
		if err != nil {
			w.logger.Error("stream closed while receiving signal", log.Error(err))
		}
		//nolint:descriptiveerrors
		return err
	case <-time.After(time.Second):
		return nil
	}
}

func (w *Worker) Stop() {
	if w.keeper != nil {
		w.keeper.Stop()
	}
	if w.slot != nil {
		w.slot.Close()
	}
	if w.publisher != nil {
		w.publisher.Stop()
	}
	if w.conn != nil {
		w.conn.Close()
	}
}

func NewSourceWrapper(
	src *PgSource,
	transferID string,
	objects *model.DataObjects,
	lgr log.Logger,
	registry *stats.SourceStats,
	cp coordinator.Coordinator,
	dbLogSnapshot bool,
) (abstract.Source, error) {
	var rollbacks util.Rollbacks
	defer rollbacks.Do()

	worker := &Worker{
		status:        make(chan error),
		src:           src,
		transferID:    transferID,
		logger:        lgr,
		metrics:       registry,
		tableLsns:     make(map[string]uint64),
		cp:            cp,
		publisher:     nil,
		keeper:        nil,
		slot:          nil,
		conn:          nil,
		objects:       objects,
		dbLogSnapshot: dbLogSnapshot,
	}
	rollbacks.Add(worker.Stop)

	connConfig, err := MakeConnConfigFromSrc(lgr, src)
	if err != nil {
		return nil, xerrors.Errorf("unable to construct connection config: %w", err)
	}
	conn, err := NewPgConnPool(connConfig, lgr)
	if err != nil {
		return nil, xerrors.Errorf("unable to construct connection pool: %w", err)
	}
	worker.conn = conn
	tracker := NewTracker(transferID, cp)
	slot, err := NewSlot(worker.conn, worker.logger, worker.src, tracker)
	if err != nil {
		return nil, xerrors.Errorf("unable to construct slot watcher: %w", err)
	}
	exist, err := slot.Exist()
	if err != nil {
		return nil, xerrors.Errorf("unable to check slot exist: %w", err)
	}
	if !exist {
		if isAurora(context.Background(), conn, lgr) {
			lgr.Warn("aurora db slot disappear, that's may be a false positive signal, wait 1 minute before retry")
			time.Sleep(time.Minute)
		}
		registry.Fatal.Inc()
		return nil, abstract.NewFatalError(xerrors.Errorf("Replication slotID %s does not exist", worker.src.SlotID))
	}
	worker.slot = slot

	keeper, err := NewKeeper(worker.conn, worker.logger, src.KeeperSchema)
	if err != nil {
		worker.logger.Warn("Unable to init keeper", log.Error(err))
		return nil, err
	}
	worker.keeper = keeper
	lgr.Info("Init new pg source")

	rollbacks.Cancel()
	return worker, nil
}
