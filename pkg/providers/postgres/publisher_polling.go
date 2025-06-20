//go:build !disable_postgres_provider

package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dustin/go-humanize"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type poller struct {
	logger          log.Logger
	conn            *pgxpool.Pool
	metrics         *stats.SourceStats
	stopCh          chan struct{}
	slotID          string
	wal2jsonParser  *Wal2JsonParser
	pollInterval    time.Duration
	lastWindow      time.Time
	batchSize       uint32
	schema          abstract.DBSchema
	once            sync.Once
	config          *PgSource
	transferID      string
	wal2jsonArgs    wal2jsonArguments
	wg              sync.WaitGroup
	slotMonitor     *SlotMonitor
	slot            AbstractSlot
	lastKeeperTime  time.Time
	pgVersion       PgVersion
	cp              coordinator.Coordinator
	changeProcessor *changeProcessor
	objects         *model.DataObjects

	skippedTables map[abstract.TableID]bool
}

const (
	slotReaderDelayQuery = `
		select now() - query_start
		from pg_stat_activity
		where pid = (select active_pid from pg_replication_slots where slot_name = $1 limit 1)
	`

	terminateStuckReaderQuery = `
		select pg_terminate_backend((
			select active_pid
			from pg_replication_slots
			where slot_name = $1
			limit 1
		))
	`

	emptyWindowToleranceTime = 100 * time.Minute
	stuckQueryCheckInterval  = 1 * time.Minute
	stuckPullInterval        = 60 * time.Minute
)

var ErrBadPostgresHealth = errors.New("postgres health problem")

type pollWindow struct {
	Left    uint32
	Right   uint32
	Size    int
	LastLSN string
}

func (p *pollWindow) Empty() bool {
	return p.Size == 0
}

func (p *poller) reloadSchema() error {
	storage, err := NewStorage(p.config.ToStorageParams(nil)) // source includes all data transfer system tables
	if err != nil {
		return xerrors.Errorf("failed to create PostgreSQL storage object at source endpoint: %w", err)
	}
	defer storage.Close()
	storage.IsHomo = true // exclude VIEWs

	tableMap, err := storage.TableList(nil)
	if err != nil {
		return xerrors.Errorf("failed to list tables (with schema) at source endpoint: %w", err)
	}

	dbSchema := tableMapToDBSchemaForTables(tableMap)
	p.schema = dbSchema

	dbSchemaToCheck, err := abstract.SchemaFilterByObjects(dbSchema, p.objects.GetIncludeObjects())
	if err != nil {
		return xerrors.Errorf("failed to filter table list extracted from source by objects set in transfer: %w", err)
	}

	if err := dbSchemaToCheck.CheckPrimaryKeys(p.config); err != nil {
		p.metrics.Fatal.Inc()
		return xerrors.Errorf("primary key check failed: %w", abstract.NewFatalError(err))
	}
	if err := coordinator.ReportFakePKey(p.cp, p.transferID, coordinator.FakePKeyStatusMessageCategory, dbSchemaToCheck.FakePkeyTables(p.config)); err != nil {
		return xerrors.Errorf("Cannot report transfer warning: %w", err)
	}

	conn, err := storage.Conn.Acquire(context.Background())
	if err != nil {
		return xerrors.Errorf("failed to acquire a connection from connection pool: %w", err)
	}
	defer conn.Release()

	changeProcessor, err := newChangeProcessor(conn.Conn(), p.schema, storage.sExTime, p.config)
	if err != nil {
		return xerrors.Errorf("unable to initialize change processor: %w", err)
	}

	p.changeProcessor = changeProcessor
	return nil
}

func (p *poller) processWindow(
	window *pollWindow,
	batchedChanges []abstract.ChangeItem,
	sink abstract.AsyncSink,
) error {
	if window.Left == 0 {
		if time.Since(p.lastWindow) > emptyWindowToleranceTime {
			p.logger.Error("empty window far too long time", log.Any("elapsed", time.Since(p.lastWindow)))
			if err := p.CheckSlot(); err != nil {
				//nolint:descriptiveerrors
				return err
			}
			//nolint:descriptiveerrors
			return ErrBadPostgresHealth
		}

		time.Sleep(p.pollInterval)
		return nil
	}

	p.lastWindow = time.Now()
	if window.Left != 0 {
		if len(batchedChanges) > 0 {
			counter := 0
			chunker := 100000
			for i := 0; i < len(batchedChanges); i += chunker {
				counter++
				end := i + chunker
				if end > len(batchedChanges) {
					end = len(batchedChanges)
				}
				err := <-sink.AsyncPush(batchedChanges[i:end])
				if err != nil {
					p.logger.Error("Sink failed", log.Error(err))
					//nolint:descriptiveerrors
					return err
				}
			}
		}

		err := backoff.Retry(func() error {
			if err := p.commitOffset(window.LastLSN, p.slotID); err != nil {
				p.logger.Warn(fmt.Sprintf("Unable to process window %v, with %v changes", window.LastLSN, len(batchedChanges)), log.Error(err))
				//nolint:descriptiveerrors
				return err
			}
			return nil
		}, backoff.WithMaxRetries(util.NewExponentialBackOff(), 5))
		if err != nil {
			p.logger.Error("Unable to commit offset", log.Error(err))
			//nolint:descriptiveerrors
			return err
		}

		if window.Size > 0 {
			p.logger.Info("Window stored", log.Any("window", window))
		}
	}

	return nil
}

func (p *poller) CheckSlot() error {
	if _, err := p.conn.Exec(context.TODO(), p.peekQ()); err != nil {
		var e *pgconn.PgError
		switch {
		case xerrors.As(err, &e):
			if e.Code == "XX000" {
				p.metrics.Fatal.Inc()
				p.logger.Error("Fatal WAL inconsistency", log.Error(e))
				return abstract.NewFatalError(err)
			}
			p.metrics.Error.Inc()
			p.logger.Error("General WAL Error", log.Error(err))
			//nolint:descriptiveerrors
			return err
		default:
			p.metrics.Error.Inc()
			p.logger.Error("General WAL Error", log.Error(err))
			//nolint:descriptiveerrors
			return err
		}
	}
	return nil
}

func (p *poller) commitOffset(lsn string, slotName string) error {
	start := time.Now()
	if _, err := p.conn.Exec(context.TODO(), "select pg_terminate_backend(active_pid) from pg_replication_slots where slot_name = $1 and active_pid is not null;", p.config.SlotID); err != nil {
		p.logger.Warn("Unable to preclean slotID for commit", log.Error(err))
	}
	q := fmt.Sprintf(
		`SELECT count(1) FROM pg_logical_slot_get_binary_changes(('%v'), ('%v'), null, %s);`,
		slotName,
		lsn,
		p.wal2jsonArgs.toSQLFormat(),
	)
	r, err := p.conn.Exec(context.TODO(), q)
	if err != nil {
		p.logger.Warnf("Unable to commit offset Query:\n%v\nError:%v", q, err)
		//nolint:descriptiveerrors
		return err
	}
	if tracker, ok := p.slot.(*LsnTrackedSlot); ok {
		if err := tracker.Move(lsn); err != nil {
			logger.Log.Error("Unable to move lsn", log.Error(err))
			//nolint:descriptiveerrors
			return err
		}
	}
	p.logger.Infof("Offset committed %v in %v (%v)", lsn, time.Since(start), r.RowsAffected())
	return nil
}

func (p *poller) Stop() {
	p.once.Do(func() {
		close(p.stopCh)
		p.wg.Wait()
		p.slotMonitor.Close()
		p.wal2jsonParser.Close()
		p.conn.Close()
	})
}

func (p *poller) Run(sink abstract.AsyncSink) error {
	slotTroubleCh := p.slotMonitor.StartSlotMonitoring(int64(p.config.SlotByteLagLimit))
	go p.monitorStuckQueries()
	defer p.wg.Done()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	p.wg.Add(2)
	for {
		select {
		case <-p.stopCh:
			p.logger.Warn("Force stop")
			return nil
		case err := <-slotTroubleCh:
			p.logger.Error("Replication slotID error", log.Error(err))
			p.metrics.Fatal.Inc()
			//nolint:descriptiveerrors
			return err
		case <-ticker.C:
			p.metrics.Master.Set(1)
			window, changes, err := p.pullChanges()
			if err != nil {
				//nolint:descriptiveerrors
				return err
			}
			if window.Left == 0 {
				p.logger.Infof("Window with no TX, need to check errors")
				if err := p.CheckSlot(); err != nil {
					p.logger.Error("Slot error", log.Error(err))
					//nolint:descriptiveerrors
					return err
				}
			}
			pushStart := time.Now()
			err = backoff.Retry(func() error {
				if err := p.processWindow(window, changes, sink); err != nil {
					p.logger.Warn(fmt.Sprintf("Unable to process window %v, with %v changes", window.LastLSN, len(changes)), log.Error(err))
					//nolint:descriptiveerrors
					return err
				}
				return nil
			}, backoff.WithMaxRetries(util.NewExponentialBackOff(), 3))
			if err != nil {
				p.logger.Error("Unable to process window", log.Error(err))
				//nolint:descriptiveerrors
				return err
			}
			p.metrics.PushTime.RecordDuration(time.Since(pushStart))
		}
	}
}

func (p *poller) pullChanges() (*pollWindow, []abstract.ChangeItem, error) {
	pullStart := time.Now()
	peekQ := p.peekQ()
	objIncleadable, err := abstract.BuildIncludeMap(p.objects.GetIncludeObjects())
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to filter table list extracted from source by objects set in transfer: %w", err)
	}
	p.logger.Debug("Start " + peekQ)
	p.logger.Debug("Start pull")
	if _, err := p.conn.Exec(context.TODO(), "select pg_terminate_backend(active_pid) from pg_replication_slots where slot_name = $1 and active_pid is not null;", p.config.SlotID); err != nil {
		p.logger.Warn("Unable to preclean slotID", log.Error(err))
	}
	rows, err := p.conn.Query(context.TODO(), peekQ)
	if err != nil {
		p.logger.Error("Get WAL", log.Error(err))
		//nolint:descriptiveerrors
		return nil, nil, err
	}
	defer rows.Close()
	var lsn string
	var xid uint32
	var batchedChanges []abstract.ChangeItem
	var counter int
	if rows.Err() != nil {
		p.logger.Error("Peek query error", log.Error(rows.Err()))
		//nolint:descriptiveerrors
		return nil, nil, rows.Err()
	}
	totalSize := 0
	parseStart := time.Now()
	var left uint32
	for rows.Next() {
		var cXid uint32
		var raw []byte
		err := rows.Scan(&lsn, &cXid, &raw)
		if err != nil {
			p.logger.Error("Scan error", log.Error(err))
			break
		}
		p.metrics.Size.Add(int64(len(raw)))
		p.metrics.Count.Inc()

		if xid < cXid {
			left = cXid
			counter = 0
			xid = cXid
		}
		lsn, _ := pglogrepl.ParseLSN(lsn)

		totalSize += len(raw)
		changes, err := p.parseChange(raw)
		if err != nil {
			//nolint:descriptiveerrors
			return nil, nil, err
		}
		if changes == nil {
			continue
		}
		for _, item := range changes {
			changeItem := item.toChangeItem()
			if !p.config.Include(changeItem.TableID()) {
				continue
			}

			if err := abstract.ValidateChangeItem(&changeItem); err != nil {
				logger.Log.Error(err.Error())
			}

			if !p.changeProcessor.hasSchemaForTable(changeItem.TableID()) {
				if p.skippedTables[changeItem.TableID()] {
					p.logger.Warn("skipping changes for a table added after replication had started", log.String("table", changeItem.TableID().String()))
					continue
				}
				if p.config.CollapseInheritTables {
					parentID, err := p.changeProcessor.resolveParentTable(context.TODO(), p.conn, changeItem.TableID())
					if err != nil {
						return nil, nil, xerrors.Errorf("unable to resolve parent: %w", err)
					}
					if p.objects != nil && len(p.objects.IncludeObjects) > 0 && !objIncleadable[parentID] {
						p.skippedTables[changeItem.TableID()] = true // to prevent next time resolve for parent
						p.logger.Warn("skipping changes for a table, since itself or its parent is not included in data-objects", log.String("table", changeItem.TableID().String()))
						continue
					}
				}
				if err := p.reloadSchema(); err != nil {
					return nil, nil, xerrors.Errorf("failed to reload schema: %w", abstract.NewFatalError(err))
				}
				if !p.changeProcessor.hasSchemaForTable(changeItem.TableID()) {
					if !p.config.IgnoreUnknownTables {
						return nil, nil, abstract.NewFatalError(xerrors.Errorf("failed to load schema for a table %s added after replication had started", changeItem.TableID().String()))
					}
					p.logger.Warn("failed to get a schema for a table added after replication started, skipping changes for this table", log.String("table", changeItem.TableID().String()))
					p.skippedTables[changeItem.TableID()] = true
					continue
				}
			}

			if err := p.changeProcessor.fixupChange(&changeItem, item.ColumnTypeOIDs, item.OldKeys.KeyTypeOids, counter, lsn); err != nil {
				//nolint:descriptiveerrors
				return nil, nil, err
			}

			if err := abstract.ValidateChangeItem(&changeItem); err != nil {
				logger.Log.Error(err.Error())
			}

			counter++

			batchedChanges = append(batchedChanges, changeItem)
		}
		if p.pgVersion.Is9x {
			p.lastKeeperTime = assignKeeperLag(changes, p.slotID, p.lastKeeperTime)
		}
	}
	if rows.Err() != nil {
		p.logger.Error("Unable to peek transactions", log.Error(rows.Err()))
		//nolint:descriptiveerrors
		return nil, nil, rows.Err()
	}
	p.logger.Info(
		fmt.Sprintf(
			"Peek %v rows in %v / %v (LSN:%v IDX:%v) %v",
			len(batchedChanges),
			time.Since(parseStart),
			time.Since(pullStart),
			lsn,
			left,
			humanize.Bytes(uint64(totalSize)),
		),
		log.Any("elapsed", time.Since(parseStart)),
	)
	p.metrics.ChangeItems.Add(int64(len(batchedChanges)))
	for _, ci := range batchedChanges {
		if ci.IsRowEvent() {
			p.metrics.Parsed.Inc()
		}
	}
	p.metrics.DecodeTime.RecordDuration(time.Since(parseStart))
	window := pollWindow{
		Left:    left,
		Right:   xid,
		Size:    len(batchedChanges),
		LastLSN: lsn,
	}
	return &window, batchedChanges, nil
}

func (p *poller) peekQ() string {
	peekQ := fmt.Sprintf(
		`SELECT * FROM pg_logical_slot_peek_binary_changes('%v', NULL, %v, %s);`,
		p.slotID,
		p.batchSize,
		p.wal2jsonArgs.toSQLFormat(),
	)
	return peekQ
}

func (p *poller) parseChange(raw []byte) ([]*Wal2JSONItem, error) {
	var changes []*Wal2JSONItem
	var err error
	changes, err = p.wal2jsonParser.Parse(raw)
	if err != nil {
		p.logger.Error("Cannot parse wal2json message", log.Error(err), log.String("data", walDataSample(raw)))
		//nolint:descriptiveerrors
		return nil, err
	}
	if changes == nil {
		return nil, nil
	}

	if err := validateChangeItemsPtrs(changes); err != nil {
		p.logger.Error(err.Error())
		//nolint:descriptiveerrors
		return nil, err
	}

	for _, change := range changes {
		changeItem := change.toChangeItem()
		if changeItem.Kind != abstract.DeleteKind && len(p.schema[changeItem.TableID()].Columns()) == 0 {
			p.logger.Info(
				"Need to load schema",
				log.Any("change_len", len(changeItem.ColumnNames)),
				log.Any("schema_len", len(p.schema[changeItem.TableID()].Columns())),
				log.Any("fqdn", changeItem.Fqtn()),
			)
			if err := p.reloadSchema(); err != nil {
				return nil, xerrors.Errorf("failed to reload schema: %w", err)
			}

			if len(p.schema[changeItem.TableID()].Columns()) == 0 {
				p.logger.Error("Schema missed in DB, make it 'fake'")
				tableColumns := make([]abstract.ColSchema, len(changeItem.ColumnValues))
				for idx := range changeItem.ColumnValues {
					var s abstract.ColSchema
					s.ColumnName = changeItem.ColumnNames[idx]
					s.DataType = "any"
					tableColumns[idx] = s
				}
				p.schema[changeItem.TableID()] = abstract.NewTableSchema(tableColumns)
			}
		}

		change.TableSchema = p.schema[changeItem.TableID()].Columns()
	}
	return changes, nil
}

func (p *poller) monitorStuckQueries() {
	defer p.wg.Done()
	ticker := time.NewTicker(stuckQueryCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			var delay time.Duration
			if err := p.conn.QueryRow(context.TODO(), slotReaderDelayQuery, p.slotID).Scan(&delay); err != nil {
				if err != pgx.ErrNoRows {
					p.logger.Warn("Cannot estimate WAL sender time lag", log.Error(err))
				}
				continue
			}
			p.logger.Infof("Slot reader is active for %v", delay)
			p.metrics.DelayTime.RecordDuration(delay)

			if delay > stuckPullInterval {
				if _, err := p.conn.Exec(context.TODO(), terminateStuckReaderQuery, p.slotID); err != nil {
					p.logger.Warn("unable to terminate backer", log.Error(err))
				}
			}
		}
	}
}

func newPollingPublisher(
	version PgVersion,
	connConfig *pgx.ConnConfig,
	slot AbstractSlot,
	wal2jsonArgs wal2jsonArguments,
	registry *stats.SourceStats,
	cfg *PgSource,
	objects *model.DataObjects,
	transferID string,
	lgr log.Logger,
	cp coordinator.Coordinator,
) (abstract.Source, error) {
	conn, err := NewPgConnPool(connConfig, lgr)
	if err != nil {
		return nil, xerrors.Errorf("unable to create conn pool: %w", err)
	}
	plr := poller{
		logger:          lgr,
		conn:            conn,
		metrics:         registry,
		stopCh:          make(chan struct{}),
		slotID:          cfg.SlotID,
		wal2jsonParser:  NewWal2JsonParser(),
		pollInterval:    200 * time.Millisecond,
		lastWindow:      time.Now(),
		batchSize:       cfg.BatchSize,
		schema:          nil,
		once:            sync.Once{},
		config:          cfg,
		transferID:      transferID,
		wal2jsonArgs:    wal2jsonArgs,
		wg:              sync.WaitGroup{},
		slotMonitor:     NewSlotMonitor(conn, cfg.SlotID, cfg.Database, registry, lgr),
		slot:            slot,
		lastKeeperTime:  time.Now(),
		pgVersion:       version,
		cp:              cp,
		changeProcessor: nil,
		objects:         objects,

		skippedTables: make(map[abstract.TableID]bool),
	}

	if err := plr.reloadSchema(); err != nil {
		plr.Stop()
		return nil, xerrors.Errorf("failed to load schema: %w", err)
	}

	return &plr, nil
}
