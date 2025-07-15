package postgres

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dustin/go-humanize"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/format"
	"github.com/transferia/transferia/pkg/parsequeue"
	sequencer2 "github.com/transferia/transferia/pkg/providers/postgres/sequencer"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type replication struct {
	logger          log.Logger
	conn            *pgxpool.Pool
	replConn        *mutexedPgConn
	metrics         *stats.SourceStats
	wal2jsonParser  *Wal2JsonParser
	error           chan error
	once            sync.Once
	config          *PgSource
	transferID      string
	schema          abstract.DBSchema
	altNames        map[abstract.TableID]abstract.TableID
	wg              sync.WaitGroup
	slotMonitor     *SlotMonitor
	stopCh          chan struct{}
	mutex           *sync.Mutex
	maxLsn          uint64
	slot            AbstractSlot
	pgVersion       PgVersion
	lastKeeperTime  time.Time
	includeCache    map[abstract.TableID]bool
	cp              coordinator.Coordinator
	sharedCtx       context.Context
	sharedCtxCancel context.CancelFunc
	changeProcessor *changeProcessor
	objects         *model.DataObjects
	sequencer       *sequencer2.Sequencer
	parseQ          *parsequeue.ParseQueue[[]abstract.ChangeItem]
	objectsMap      map[abstract.TableID]bool //tables to include in transfer

	skippedTables map[abstract.TableID]bool
}

var pgFatalCode = map[string]bool{
	"XX000": true, // TM-1332
	"58P01": true, // TM-2082
	"55000": true, // TRANSFER-145 Object_not_in_prerequisite_state
}

const BufferLimit = 16 * humanize.MiByte

func (p *replication) Run(sink abstract.AsyncSink) error {
	var err error
	//level of parallelism combined with hardcoded buffer size in receiver(16mb) prevent OOM in parsequeue
	p.parseQ = parsequeue.New(p.logger, 10, sink, p.WithIncludeFilter, p.ack)

	if err = p.reloadSchema(); err != nil {
		return xerrors.Errorf("failed to load schema: %w", err)
	}

	includedObjects := p.objects.GetIncludeObjects()
	if len(includedObjects) > 0 {
		includedObjects = append(includedObjects, p.config.AuxTables()...)
	}

	if p.objectsMap, err = abstract.BuildIncludeMap(includedObjects); err != nil {
		return xerrors.Errorf("unable to build transfer data-objects: %w", err)
	}

	slotTroubleCh := p.slotMonitor.StartSlotMonitoring(int64(p.config.SlotByteLagLimit))

	p.wg.Add(2)
	go p.receiver(slotTroubleCh)
	go p.standbyStatus()
	select {
	case err := <-p.error:
		return err
	case <-p.stopCh:
		return nil
	}
}

func (p *replication) Stop() {
	p.once.Do(func() {
		close(p.stopCh)
		p.sharedCtxCancel()
		if err := p.replConn.Close(context.Background()); err != nil {
			p.logger.Error("Cannot close replication connection", log.Error(err))
		}
		p.wg.Wait()
		p.slotMonitor.Close()
		p.wal2jsonParser.Close()
		p.conn.Close()
		p.parseQ.Close()
	})
}

func (p *replication) sendError(err error) {
	select {
	case p.error <- err:
		p.logger.Error("error, stop replication", log.Error(err))
	case <-p.stopCh:
		if err != nil {
			p.logger.Warn("error after replication stopped", log.Error(err))
		}
	}
}

func (p *replication) ack(data []abstract.ChangeItem, pushSt time.Time, err error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !util.IsOpen(p.stopCh) {
		return
	}

	if err != nil {
		p.sendError(err)
		return
	}

	if committedLsn, err := p.sequencer.Pushed(data); err != nil {
		logger.Log.Error("sequence of processed changeItems is incorrect", log.Error(err))
		p.sendError(err)
		return
	} else {
		p.maxLsn = committedLsn
	}

	p.metrics.PushTime.RecordDuration(time.Since(pushSt))
}

func (p *replication) WithIncludeFilter(items []abstract.ChangeItem) []abstract.ChangeItem {
	var changes []abstract.ChangeItem
	for _, change := range items {
		if _, ok := p.includeCache[change.TableID()]; !ok {
			p.includeCache[change.TableID()] = p.config.Include(change.TableID())
		}
		if len(p.objectsMap) > 0 { // if we have transfer include objects we should strictly push only them
			_, tablePresent := p.objectsMap[change.TableID()]
			// Let's imagine that we work with partitioned tables and CollapseInheritTables is on
			// How our code works: 1) we rename partitioned tables using transformers and this happens when we push ChangeItems
			// 2) IncludeObjects may include name of parent table(this is how it worked previously so this behaviour should be preserved)
			// 3) therefore when we check if table is present in IncludeObjects we should also check if it's parent is present
			// otherwise we will just skip all partition tables during replication
			_, parentPresent := p.objectsMap[p.altNames[change.TableID()]]
			if !parentPresent && !tablePresent {
				continue
			}
		}
		if p.includeCache[change.TableID()] {
			changes = append(changes, change)
		}
	}
	return changes
}

func (p *replication) reloadSchema() error {
	storage, err := NewStorage(p.config.ToStorageParams(nil)) // source includes all data transfer system tables
	if err != nil {
		return xerrors.Errorf("failed to create PostgreSQL storage object at source endpoint: %w", err)
	}
	defer storage.Close()
	storage.IsHomo = true // exclude VIEWs. This is a nasty solution which should be replaced when an Accessor is introduced instead of the jack of all trades Storage

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

	if p.config.CollapseInheritTables {
		childParentMap, err := MakeChildParentMap(p.sharedCtx, p.conn)
		if err != nil {
			return xerrors.Errorf("failed while reading pg_inherits: %w", err)
		}
		p.altNames = childParentMap
	}

	if err := dbSchemaToCheck.CheckPrimaryKeys(p.config); err != nil {
		p.metrics.Fatal.Inc()
		return xerrors.Errorf("primary key check failed: %w", abstract.NewFatalError(err))
	}
	if err := coordinator.ReportFakePKey(p.cp, p.transferID, coordinator.FakePKeyStatusMessageCategory, dbSchemaToCheck.FakePkeyTables(p.config)); err != nil {
		return xerrors.Errorf("Cannot report transfer warning: %w", err)
	}

	conn, err := storage.Conn.Acquire(p.sharedCtx)
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

const FakeParentPKeyStatusMessageCategory string = "fake_primary_key_parent"

// tableMapToDBSchemaForTables converts one type of schema to another ONLY for tables (dropping VIEWs)
func tableMapToDBSchemaForTables(tableMap abstract.TableMap) abstract.DBSchema {
	result := make(abstract.DBSchema)
	for id, info := range tableMap {
		if info.IsView {
			continue
		}
		result[id] = info.Schema
	}
	return result
}

func (p *replication) standbyStatus() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	defer p.wg.Done()

	var lastLsn uint64

	for range ticker.C {
		select {
		case <-p.stopCh:
			return
		default:
		}
		p.mutex.Lock()
		copiedMaxLsn := p.maxLsn
		p.mutex.Unlock()

		if lastLsn != copiedMaxLsn {
			lastLsn = copiedMaxLsn
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
		statusUpdate := pglogrepl.StandbyStatusUpdate{WALWritePosition: pglogrepl.LSN(copiedMaxLsn)}
		if err := p.replConn.SendStandbyStatusUpdate(ctx, statusUpdate); err != nil {
			logger.Log.Warn("Unable to send standby status", log.Error(err))
		} else {
			p.logger.Infof("Heartbeat send %v", copiedMaxLsn)
		}
		if tracker, ok := p.slot.(*LsnTrackedSlot); ok && copiedMaxLsn > 0 {
			var restartLsn string
			if err := p.conn.QueryRow(ctx, SelectLsnForSlot, p.transferID).Scan(&restartLsn); err != nil {
				logger.Log.Warn("Unable to get restart lsn", log.Error(err))
			} else if err := tracker.Move(restartLsn); err != nil {
				logger.Log.Warn("Unable to move lsn", log.Error(err))
			}
		}
		cancel()
	}
}

func (p *replication) receiver(slotTroubleCh <-chan error) {
	defer p.wg.Done()
	defer logger.Log.Info("Receiver stopped")
	var lastLsn uint64
	var cTime time.Time
	parsed := true
	var lastMessageTime time.Time
	var messageCounter int
	var data []*pglogrepl.XLogData
	bufferSize := model.BytesSize(0)
	for {
		select {
		case <-p.stopCh:
			logger.Log.Warn("Force stop")
			return
		case err := <-slotTroubleCh:
			p.logger.Error("Replication slotID error", log.Error(err))
			p.metrics.Fatal.Inc()
			p.sendError(err)
			return
		default:
		}
		p.metrics.Master.Set(1)

		backendMessage, err := p.replConn.ReceiveMessage(p.sharedCtx, p.slotMonitor)
		if err != nil {
			if xerrors.Is(err, context.Canceled) {
				return
			}
			var pgErr *pgconn.PgError
			if xerrors.As(err, &pgErr) && pgFatalCode[pgErr.Code] {
				p.logger.Error("Pg fatal error", log.Error(err))
				p.metrics.Fatal.Inc()
				p.sendError(abstract.NewFatalError(err))
				return
			}
			p.logger.Warn("Connection dropped", log.Error(err))
			p.sendError(err)
			return
		}

		message, ok := backendMessage.(*pgproto3.CopyData)
		if !ok {
			// Happens when PostgreSQL sends an information message, like a warning
			continue
		}

		switch message.Data[0] {
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(message.Data[1:])
			if err != nil {
				p.sendError(xerrors.Errorf("unknown pg message: %w", err))
				return
			}
			messageCounter++
			p.metrics.Size.Add(int64(len(xld.WALData)))
			p.metrics.Count.Inc()
			p.metrics.DelayTime.RecordDuration(time.Since(xld.ServerTime))

			transactionComplete := string(xld.WALData) == "]}"
			shouldFlush := (bufferSize > BufferLimit) || transactionComplete

			if !parsed && bufferSize > BufferLimit {
				for !parsed {
					time.Sleep(time.Second)
					p.logger.Infof("buffer size too large while data inflight, throttle read: %v", format.SizeInt(int(bufferSize)))
				}
			}

			data = append(data, &xld)
			if shouldFlush && parsed {
				parsed = false
				go func(data []*pglogrepl.XLogData, messageCounter int) {
					defer func() {
						parsed = true
					}()
					var res []abstract.ChangeItem
					for _, d := range data {
						changeItems, err := p.parseWal2JsonChanges(p.changeProcessor, d)
						if err != nil {
							p.sendError(xerrors.Errorf("Cannot parse logical replication message: %w", err))
							return
						}
						res = append(res, changeItems...)
					}
					if len(res) == 0 {
						return
					}
					if time.Since(lastMessageTime) > 5*time.Second {
						xldString := fmt.Sprintf("Wal: %s Time: %s Lag: %d", xld.WALStart, xld.ServerTime, uint64(xld.ServerWALEnd-xld.WALStart))
						p.logger.Infof(
							"Read rows %v %v (oldest %v newest %v count %d)",
							xldString,
							humanize.Bytes(uint64(len(xld.WALData))),
							time.Unix(0, int64(res[0].CommitTime)),
							time.Unix(0, int64(res[len(res)-1].CommitTime)),
							messageCounter,
						)
						lastMessageTime = time.Now()
					}
					p.metrics.ChangeItems.Add(int64(len(res)))
					for _, ci := range res {
						if ci.IsRowEvent() {
							p.metrics.Parsed.Inc()
						}
					}

					if err = p.sequencer.StartProcessing(res); err != nil {
						p.sendError(xerrors.Errorf("unable to start processing: %w", err))
						return
					}
					if err = p.parseQ.Add(res); err != nil {
						p.sendError(xerrors.Errorf("unable to add to pusher q: %w", err))
						return
					}
					cTime = time.Unix(0, int64(res[0].CommitTime))
				}(data, messageCounter)
				data = []*pglogrepl.XLogData{}
				bufferSize = 0
				messageCounter = 0
			} else {
				bufferSize += model.BytesSize(len(xld.WALData))
			}
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			p.mutex.Lock()
			copiedMaxLsn := p.maxLsn
			p.mutex.Unlock()

			if lastLsn != copiedMaxLsn {
				p.logger.Infof("Heartbeat %v at %v", copiedMaxLsn, cTime)
				lastLsn = copiedMaxLsn
			}
		}
	}
}

func (p *replication) parseWal2JsonChanges(cp *changeProcessor, xld *pglogrepl.XLogData) ([]abstract.ChangeItem, error) {
	st := time.Now()
	items, err := p.wal2jsonParser.Parse(xld.WALData)
	if err != nil {
		logger.Log.Error("Cannot parse wal2json message", log.Error(err), log.String("data", walDataSample(xld.WALData)))
		return nil, xerrors.Errorf("Cannot parse wal2json message: %w", err)
	}
	objIncleadable, err := abstract.BuildIncludeMap(p.objects.GetIncludeObjects())
	if err != nil {
		return nil, xerrors.Errorf("failed to filter table list extracted from source by objects set in transfer: %w", err)
	}
	if err := validateChangeItemsPtrs(items); err != nil {
		p.logger.Error(err.Error())
		//nolint:descriptiveerrors
		return nil, err
	}
	changes := make([]abstract.ChangeItem, 0, 1)
	for i, item := range items {
		changeItem := item.toChangeItem()
		if err := abstract.ValidateChangeItem(&changeItem); err != nil {
			logger.Log.Error(err.Error())
		}
		if !cp.hasSchemaForTable(changeItem.TableID()) {
			if p.skippedTables[changeItem.TableID()] {
				p.logger.Debug("skipping changes for a table added after replication had started", log.String("table", changeItem.TableID().String()))
				continue
			}
			if p.config.CollapseInheritTables {
				parentID, err := p.changeProcessor.resolveParentTable(p.sharedCtx, p.conn, changeItem.TableID())
				if err != nil {
					return nil, xerrors.Errorf("unable to resolve parent: %w", err)
				}
				if p.objects != nil && len(p.objects.IncludeObjects) > 0 && !objIncleadable[parentID] {
					p.skippedTables[changeItem.TableID()] = true // to prevent next time resolve for parent
					p.logger.Warn(
						"skipping changes for a table, since itself or its parent is not included in data-objects",
						log.String("table", changeItem.TableID().String()),
						log.String("parent_table", parentID.String()),
					)
					continue
				}
			}
			if err := p.reloadSchema(); err != nil {
				return nil, xerrors.Errorf("failed to reload schema: %w", abstract.NewFatalError(err))
			}
			if !cp.hasSchemaForTable(changeItem.TableID()) {
				if !p.config.IgnoreUnknownTables {
					return nil, xerrors.Errorf("failed to load schema for a table %s added after replication had started", changeItem.TableID().String())
				}
				p.logger.Warn("failed to get a schema for a table added after replication started, skipping changes for this table", log.String("table", changeItem.TableID().String()))
				p.skippedTables[changeItem.TableID()] = true
				continue
			}
		}
		if err := cp.fixupChange(&changeItem, item.ColumnTypeOIDs, item.OldKeys.KeyTypeOids, i, xld.WALStart); err != nil {
			//nolint:descriptiveerrors
			return nil, err
		}

		if err := abstract.ValidateChangeItem(&changeItem); err != nil {
			logger.Log.Error(err.Error())
		}
		changes = append(changes, changeItem)
	}
	if p.pgVersion.Is9x {
		p.lastKeeperTime = assignKeeperLag(items, p.config.SlotID, p.lastKeeperTime)
	}

	p.metrics.DecodeTime.RecordDuration(time.Since(st))
	return changes, nil
}

func newReplicationPublisher(
	version PgVersion,
	connConfig *pgx.ConnConfig,
	slot AbstractSlot,
	wal2jsonArgs wal2jsonArguments,
	stats *stats.SourceStats,
	source *PgSource,
	transferID string,
	lgr log.Logger,
	cp coordinator.Coordinator,
	objects *model.DataObjects,
) (abstract.Source, error) {
	var rb util.Rollbacks
	defer rb.Do()
	replConn, err := startReplication(connConfig, version, source.SlotID, wal2jsonArgs, lgr)
	if err != nil {
		return nil, xerrors.Errorf("unable to start replication: %w", err)
	}
	rb.Add(func() {
		if err := replConn.Close(context.Background()); err != nil {
			lgr.Error("Cannot close replication connection", log.Error(err))
		}
	})
	connPool, err := NewPgConnPool(connConfig, lgr)
	if err != nil {
		return nil, xerrors.Errorf("unable to create conn pool: %w", err)
	}

	rb.Cancel()
	ctx, cancel := context.WithCancel(context.Background())
	return &replication{
		logger:          lgr,
		conn:            connPool,
		replConn:        replConn,
		metrics:         stats,
		wal2jsonParser:  NewWal2JsonParser(),
		error:           make(chan error, 1),
		once:            sync.Once{},
		config:          source,
		transferID:      transferID,
		schema:          nil,
		altNames:        nil,
		wg:              sync.WaitGroup{},
		slotMonitor:     NewSlotMonitor(connPool, source.SlotID, source.Database, stats, lgr),
		stopCh:          make(chan struct{}),
		mutex:           new(sync.Mutex),
		maxLsn:          0,
		slot:            slot,
		pgVersion:       version,
		lastKeeperTime:  time.Now(),
		includeCache:    map[abstract.TableID]bool{},
		cp:              cp,
		changeProcessor: nil,
		sharedCtx:       ctx,
		sharedCtxCancel: cancel,
		objects:         objects,
		sequencer:       sequencer2.NewSequencer(),
		parseQ:          nil,
		objectsMap:      nil,

		skippedTables: make(map[abstract.TableID]bool),
	}, nil
}

func startReplication(
	connConfig *pgx.ConnConfig,
	version PgVersion,
	slotName string,
	wal2jsonArgs wal2jsonArguments,
	lgr log.Logger,
) (*mutexedPgConn, error) {
	return backoff.RetryNotifyWithData(func() (*mutexedPgConn, error) {
		var rb util.Rollbacks
		defer rb.Do()
		rConnConfig := makeReplicationConnConfig(connConfig.Config, version)
		rConn, err := newReplicationConnection(rConnConfig)
		if err != nil {
			// Protocol violation, means that database do not accept replication protocol. Do not retry this case.
			if strings.Contains(err.Error(), "08P01") {
				//nolint:descriptiveerrors
				return nil, backoff.Permanent(err)
			}
			return nil, xerrors.Errorf("error establishing replication connection: %w", err)
		}
		rb.Add(func() {
			if err := rConn.Close(context.Background()); err != nil {
				lgr.Error("Cannot close replication connection", log.Error(err))
			}
		})

		lgr.Infof("Start replication process with args: %v", wal2jsonArgs)
		err = rConn.StartReplication(context.Background(), slotName, 0, pglogrepl.StartReplicationOptions{
			Timeline:   -1,
			Mode:       pglogrepl.LogicalReplication,
			PluginArgs: wal2jsonArgs.toReplicationFormat(),
		})
		if err != nil {
			defer lgr.Warn("Cannot start replication via replication connection", log.Error(err))
			// usually that means slot has been invalidated by some condition (e.g. max_wal_slot_keep_size setting or smth)
			if strings.Contains(err.Error(), "SQLSTATE 55000") {
				//nolint:descriptiveerrors
				return nil, backoff.Permanent(abstract.NewFatalError(
					xerrors.Errorf("Cannot start replication via replication connection: %w", err)))
			}
			// object_in_use code means some other process is reading the slot
			// nobody is expected to read transfer slot so most common case of this error is stale transfer process
			if strings.Contains(err.Error(), "SQLSTATE 55006") {
				tryKillSlotReader(rConn, slotName, lgr)
			}
			//nolint:descriptiveerrors
			return nil, err
		}
		rb.Cancel()
		return rConn, nil
	}, backoff.WithMaxRetries(util.NewExponentialBackOff(), 5), util.BackoffLoggerWarn(lgr, "cannot start replication"))
}

func makeReplicationConnConfig(srcConfig pgconn.Config, version PgVersion) *pgconn.Config {
	// It's important to copy config before mutating RuntimeParams
	// otherwise those params may accidentaly be shared among different connection (pools)
	rConnConfig := srcConfig.Copy()
	if rConnConfig.RuntimeParams == nil {
		rConnConfig.RuntimeParams = make(map[string]string)
	}
	if !version.Is9x && !version.Is10x && !version.Is11x {
		rConnConfig.RuntimeParams["options"] = "-c wal_sender_timeout=3600000"
	}
	rConnConfig.RuntimeParams["replication"] = "database"
	return rConnConfig
}

func newReplicationConnection(rConnConfig *pgconn.Config) (*mutexedPgConn, error) {
	rConnRaw, err := pgconn.ConnectConfig(context.TODO(), rConnConfig)
	if err != nil {
		return nil, err
	}
	return newMutexedPgConn(rConnRaw), nil
}

func tryKillSlotReader(conn *mutexedPgConn, slotName string, lgr log.Logger) {
	sql := fmt.Sprintf(`SELECT PG_TERMINATE_BACKEND(active_pid) FROM pg_replication_slots
WHERE slot_name = '%v' AND active_pid IS NOT NULL;`, slotName)
	reader := conn.Exec(context.Background(), sql)
	if _, readerErr := reader.ReadAll(); readerErr != nil {
		lgr.Warn(fmt.Sprintf("Unable to terminate reader for slot %s", slotName), log.Error(readerErr))
	} else {
		lgr.Infof("Reader for slot %s has been terminated", slotName)
	}
}
