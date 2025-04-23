package mysql

import (
	"crypto/tls"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	default_mysql "github.com/go-sql-driver/mysql"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/format"
	unmarshaller "github.com/transferia/transferia/pkg/providers/mysql/unmarshaller/replication"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/compat/golog"
)

func init() {
	golog.SetLevel(log.FatalLevel)
}

type binlogHandler struct {
	inflight         []abstract.ChangeItem
	logger           log.Logger
	metrics          *stats.SourceStats
	sink             abstract.AsyncSink
	inflightSize     uint32
	bufferLimit      uint32
	rw               sync.Mutex
	config           *MysqlSource
	connectionParams *ConnectionParams
	tracker          *Tracker
	hasSystemDDL     bool
	nextPos          mysql.Position
	expressions      map[string]string
	requiredCols     map[string]map[string]bool
	canal            *Canal
	isClosed         bool
}

func (h *binlogHandler) Close() error {
	h.isClosed = true
	return h.tracker.Close()
}

func (h *binlogHandler) OnRotate(rotateEvent *replication.RotateEvent) error {
	h.logger.Info("rotate", log.Any("event", rotateEvent))
	return nil
}

func (h *binlogHandler) OnTableChanged(schema string, table string) error {
	h.logger.Warnf("DDL detected on %v.%v", schema, table)
	return nil
}

func (h *binlogHandler) OnGTID(gtid mysql.GTIDSet) error {
	return nil
}

func (h *binlogHandler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	h.logger.Debug("OnPosSynced", log.Any("event", set), log.Any("position", pos), log.Any("force", force))
	h.nextPos = pos
	return nil
}

func (h *binlogHandler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	if strings.Contains(string(queryEvent.Query), "create table if not exists __tm_keeper (") ||
		strings.Contains(string(queryEvent.Query), "create table if not exists __tm_gtid_keeper (") {

		if h.hasSystemDDL {
			return nil
		}
		h.hasSystemDDL = true
	}
	for {
		if h.isClosed {
			h.logger.Warn("unable to proceed DDL, handler is closed", log.Any("event", queryEvent))
			return xerrors.New("handler is closed")
		}
		if len(h.inflight) == 0 {
			break
		}
		time.Sleep(h.config.ReplicationFlushInterval)
	}
	h.logger.Warn("DDL Query", log.Any("query", string(queryEvent.Query)), log.Any("position", nextPos))
	execTS := time.Now()
	txSequence := nextPos.Pos
	gtidID := ""
	if gtid := h.canal.SyncedGTIDSet(); gtid != nil {
		hash := fnv.New32()
		_, _ = hash.Write([]byte(gtid.String()))
		txSequence = hash.Sum32()
		gtidID = gtid.String()
		h.logger.Infof("on ddl for TX:%v (%v): %v", gtid.String(), txSequence, string(queryEvent.Query))
	}
	ddl := []abstract.ChangeItem{{
		ID:           txSequence,
		TxID:         gtidID,
		CommitTime:   uint64(execTS.UnixNano()),
		Kind:         abstract.DDLKind,
		ColumnNames:  []string{"query"},
		ColumnValues: []interface{}{string(queryEvent.Query)},
		TableSchema:  abstract.NewTableSchema([]abstract.ColSchema{{}}),
		Size:         abstract.RawEventSize(uint64(len(queryEvent.Query))),
	}}
	h.metrics.ChangeItems.Inc()
	if err := <-h.sink.AsyncPush(ddl); err != nil {
		h.metrics.Error.Inc()
		// Error code under 1000 is global error (usually network errors), above 2000 - client error codes;
		// Error code [1000, 2000) - these are server error codes, that can be skipped;
		var mySQLErr *default_mysql.MySQLError
		if xerrors.As(err, &mySQLErr) && mySQLErr.Number >= 1000 && mySQLErr.Number < 2000 {
			h.metrics.DDLError.Inc()
			h.logger.Warn("Unable to apply ddl", log.Error(err), log.Any("ddl", string(queryEvent.Query)))
			return nil
		}
		return xerrors.Errorf("sinker push failed: %w", err)
	}
	return nil
}

func (h *binlogHandler) OnXID(nextPos mysql.Position) error {
	h.logger.Debug("OnXID", log.Any("position", nextPos))
	h.nextPos = nextPos
	return nil
}

func (h *binlogHandler) OnRow(event *RowsEvent) error {
	throttled := false
	throtleSt := time.Now()
	for {
		if h.isClosed {
			h.logger.Warn("unable to proceed event, handler is closed", log.Any("event", event))
			return xerrors.New("handler is closed")
		}
		if h.inflightSize < h.bufferLimit {
			if throttled {
				h.logger.Infof("throttled for %v", time.Since(throtleSt))
			}
			break
		}
		if !throttled {
			h.logger.Infof("too big buffer, throttle read (%v / %v)", format.SizeUInt32(h.inflightSize), format.SizeUInt32(h.config.BufferLimit))
			throttled = true
		}
		time.Sleep(time.Millisecond * 100)
	}
	h.logger.Debugf("input row %v", event.Header)
	h.metrics.Size.Add(int64(event.Header.EventSize))
	h.metrics.Count.Inc()
	// base value for canal.DeleteAction or canal.InsertAction or canal.UpdateAction
	n := 0
	k := 1

	if event.Action == UpdateAction {
		n = 1
		k = 2
	}

	start := time.Now()

	if !h.config.IsHomo {
		if err := CastRowsToDT(event, h.connectionParams.Location, unmarshaller.UnmarshalHetero); err != nil {
			return xerrors.Errorf("failed to cast mysql binlog event to YT rows: %w", err)
		}
	} else {
		if err := CastRowsToDT(event, h.connectionParams.Location, unmarshaller.UnmarshalHomo); err != nil {
			return xerrors.Errorf("failed to cast mysql binlog event to Mysql rows: %w", err)
		}
	}

	res := make([]abstract.ChangeItem, 0)
	reqCols, err := h.getRequiredCols(event.Table.Schema, event.Table.Name)
	if err != nil {
		return xerrors.Errorf("failed to get required columns: %w", err)
	}
	schEvent := newSchematizedRowsEvent(event, h.expressions, reqCols)
	colNames := schEvent.ColumnNames()
	sch := schEvent.Schema()

	lsn := CalculateLSN(h.nextPos.Name, uint64(event.Header.LogPos))
	txSequence := event.Header.Timestamp
	gtidStr := ""
	if gtid := h.canal.SyncedGTIDSet(); gtid != nil {
		hash := fnv.New32()
		_, _ = hash.Write([]byte(gtid.String()))
		gtidStr = gtid.String()
		txSequence = hash.Sum32()
		h.logger.Debugf("on row for TX:%v (%v) with %v changes", gtid.String(), txSequence, len(event.Data.Rows))
	}

	rowsPerItem := k
	itemsNumber := len(event.Data.Rows) / rowsPerItem
	rawItemEtaSize := uint64(int(event.Header.EventSize) / itemsNumber)

	switch event.Action {
	case UpdateAction:
		// supports all variants of binlog_row_image
		for i := n; i < len(event.Data.Rows); i += k {
			if event.IsAllColumnsPresent1() && event.IsAllColumnsPresent2() {
				res = append(res, abstract.ChangeItem{
					ID:           txSequence,
					TxID:         gtidStr,
					LSN:          lsn,
					CommitTime:   uint64(time.Unix(int64(event.Header.Timestamp), 0).UnixNano()),
					Counter:      i,
					Kind:         abstract.UpdateKind,
					Schema:       event.Table.Schema,
					Table:        event.Table.Name,
					PartID:       "",
					ColumnNames:  colNames,
					ColumnValues: schEvent.GetRowValues(i),
					TableSchema:  sch,
					OldKeys: abstract.OldKeysType{
						KeyNames:  colNames,
						KeyTypes:  nil,
						KeyValues: schEvent.GetRowValues(i - 1), // it's contract of mysql; even line - old values for full line. odd line - new values
					},
					Query: event.Query,
					Size:  abstract.RawEventSize(rawItemEtaSize),
				})
			} else {
				cs := abstract.ChangeItem{
					ID:           txSequence,
					LSN:          lsn,
					CommitTime:   uint64(time.Unix(int64(event.Header.Timestamp), 0).UnixNano()),
					Counter:      i,
					Kind:         abstract.UpdateKind,
					Schema:       event.Table.Schema,
					Table:        event.Table.Name,
					PartID:       "",
					ColumnNames:  nil,
					ColumnValues: nil,
					TableSchema:  sch,
					OldKeys:      *new(abstract.OldKeysType),
					TxID:         gtidStr,
					Query:        event.Query,
					Size:         abstract.RawEventSize(rawItemEtaSize),
				}
				cols := make([]string, 0)
				vals := make([]interface{}, 0)
				oldCols := make([]string, 0)
				oldVals := make([]interface{}, 0)
				for _, col := range colNames {
					hasOldValue := schEvent.HasOldValue(col)
					hasNewValue := schEvent.HasNewValue(col)
					if !hasOldValue && !hasNewValue {
						continue
					}
					cols = append(cols, col)
					vals = append(vals, nil)
					if hasOldValue {
						vals[len(vals)-1] = schEvent.GetColumnValue(i-1, col)
						oldCols = append(oldCols, col)
						oldVals = append(oldVals, schEvent.GetColumnValue(i-1, col))
					}
					if hasNewValue {
						vals[len(vals)-1] = schEvent.GetColumnValue(i, col)
					}
				}
				cs.ColumnNames = cols
				cs.ColumnValues = vals
				cs.OldKeys.KeyNames = oldCols
				cs.OldKeys.KeyValues = oldVals
				res = append(res, cs)
			}
		}
	case DeleteAction:
		for i := n; i < len(event.Data.Rows); i += k {
			vals := schEvent.GetRowValues(i)
			c := &abstract.ChangeItem{
				ID:           txSequence,
				LSN:          lsn,
				TxID:         gtidStr,
				CommitTime:   uint64(time.Unix(int64(event.Header.Timestamp), 0).UnixNano()),
				Counter:      i,
				Kind:         abstract.DeleteKind,
				Schema:       event.Table.Schema,
				Table:        event.Table.Name,
				PartID:       "",
				ColumnNames:  colNames,
				ColumnValues: vals,
				TableSchema:  sch,
				OldKeys: abstract.OldKeysType{
					KeyNames:  colNames,
					KeyTypes:  nil,
					KeyValues: vals,
				},
				Query: event.Query,
				Size:  abstract.RawEventSize(rawItemEtaSize),
			}
			keys := make([]string, 0)
			keyTypes := make([]string, 0)
			keyVals := make([]interface{}, 0)
			for i, col := range sch.Columns() {
				if col.PrimaryKey {
					keys = append(keys, col.ColumnName)
					keyVals = append(keyVals, c.ColumnValues[i])
					keyTypes = append(keyTypes, col.DataType)
				}
			}
			c.OldKeys = abstract.OldKeysType{
				KeyNames:  keys,
				KeyTypes:  keyTypes,
				KeyValues: keyVals,
			}
			res = append(res, *c)
		}
	case InsertAction:
		for i := n; i < len(event.Data.Rows); i += k {
			res = append(res, abstract.ChangeItem{
				ID:           txSequence,
				LSN:          lsn,
				CommitTime:   uint64(time.Unix(int64(event.Header.Timestamp), 0).UnixNano()),
				Counter:      i,
				Kind:         abstract.InsertKind,
				Schema:       event.Table.Schema,
				Table:        event.Table.Name,
				PartID:       "",
				ColumnNames:  colNames,
				ColumnValues: schEvent.GetRowValues(i),
				TableSchema:  sch,
				OldKeys:      *new(abstract.OldKeysType),
				TxID:         gtidStr,
				Query:        event.Query,
				Size:         abstract.RawEventSize(rawItemEtaSize),
			})
		}
	default:
		h.metrics.Unparsed.Add(int64(len(event.Data.Rows)))
		return xerrors.Errorf("unknown canal action %q", event.Action)
	}

	if err := abstract.ValidateChangeItems(res); err != nil {
		h.logger.Error("failed to validate ChangeItems", log.Error(err))
		return xerrors.Errorf("failed to validate ChangeItems: %w", err)
	}
	h.metrics.DecodeTime.RecordDuration(time.Since(start))
	h.metrics.ChangeItems.Add(int64(len(res)))
	for _, ci := range res {
		if ci.IsRowEvent() {
			h.metrics.Parsed.Inc()
		}
	}
	h.logger.Debugf("add commit %v from %v", event.Header.EventType, time.Unix(int64(event.Header.Timestamp), 0))
	h.rw.Lock()
	defer h.rw.Unlock()
	h.inflightSize += event.Header.EventSize
	h.inflight = append(h.inflight, res...)
	return nil
}

func (h *binlogHandler) String() string {
	return fmt.Sprintf("%v_%v", h.connectionParams.ClusterID, h.config.ServerID)
}

func (h *binlogHandler) getRequiredCols(schema, table string) (map[string]bool, error) {
	name := fmt.Sprintf("`%s`.`%s`", schema, table)
	if sc, ok := h.requiredCols[name]; ok {
		return sc, nil
	}
	r, err := h.canal.Execute(fmt.Sprintf("show full columns from `%s`.`%s`", schema, table))
	if err != nil {
		return nil, xerrors.Errorf("failed to execute column list SQL query: %w", err)
	}

	s := map[string]bool{}
	for i := 0; i < r.RowNumber(); i++ {
		col, _ := r.GetString(i, 0)
		req, _ := r.GetString(i, 3)
		s[col] = req == "NO"
	}
	h.requiredCols[name] = s

	return s, nil
}

type publisher struct {
	logger          log.Logger
	metrics         metrics.Registry
	stopCh          chan bool
	stopped         bool
	once            sync.Once
	canal           *Canal
	handler         *binlogHandler
	tracker         *Tracker
	config          *MysqlSource
	transferID      string
	gtidReplication bool
	cp              coordinator.Coordinator
	objects         *model.DataObjects
	flusherErr      error
	storage         *Storage
}

func (p *publisher) Run(sink abstract.AsyncSink) error {
	sch, err := LoadSchema(p.storage.DB, p.config.UseFakePrimaryKey, false)
	if err != nil {
		return xerrors.Errorf("failed to load schema: %w", err)
	}
	sch, err = abstract.SchemaFilterByObjects(sch, p.objects.GetIncludeObjects())
	if err != nil {
		return xerrors.Errorf("schema filter failed: %w", err)
	}
	if err := sch.CheckPrimaryKeys(p.config); err != nil {
		p.handler.metrics.Fatal.Inc()
		return abstract.NewFatalError(err)
	}
	if err := coordinator.ReportFakePKey(p.cp, p.transferID, coordinator.FakePKeyStatusMessageCategory, sch.FakePkeyTables(p.config)); err != nil {
		return xerrors.Errorf("failed to report transfer warning: %w", err)
	}

	p.handler.expressions = map[string]string{}
	for _, table := range sch {
		for _, column := range table.Columns() {
			if column.Expression != "" {
				p.handler.expressions[fmt.Sprintf("%v.%v", column.TableName, column.ColumnName)] = column.Expression
			}
		}
	}
	p.handler.sink = sink

	if p.gtidReplication {
		gtid, err := p.tracker.GetGtidset()
		if err != nil || gtid == nil {
			return xerrors.Errorf("Cannot get gtidset: %w", err)
		}

		p.logger.Infof("Init reader from gtidset: %v", gtid)
		if err := p.canal.StartFromGTID(gtid); err != nil {
			cErr := util.Unwrap(err)
			var mErr *mysql.MyError
			if xerrors.As(cErr, &mErr) {
				if mErr.Code == mysql.ER_MASTER_FATAL_ERROR_READING_BINLOG {
					p.logger.Error("fatal canal error", log.Error(mErr))
					return xerrors.Errorf("fatal canal error: %w", abstract.NewFatalError(err))
				}
			}
			p.logger.Error("canal run failed", log.Error(err))
			return xerrors.Errorf("failed to run canal: %w", err)
		}
	} else {
		name, pos, err := p.tracker.Get()
		if err != nil {
			return xerrors.Errorf("failed to get binlog tracker: %w", err)
		}
		if name == "" && pos == 0 {
			p.logger.Errorf("Mysql tracker has no binlog position for server_id: %v:%v", p.storage.ConnectionParams.Host, p.config.ServerID)
			return xerrors.Errorf("MySQL tracker has no binlog position: %w", abstract.NewFatalError(xerrors.New("Binlog position not found in binlog tracker")))
		}

		p.logger.Infof("Init reader from filename %v:%v", name, pos)
		if err := p.canal.RunFrom(mysql.Position{
			Name: name,
			Pos:  pos,
		}); err != nil {
			cErr := util.Unwrap(err)
			var mErr *mysql.MyError
			if xerrors.As(cErr, &mErr) {
				if mErr.Code == mysql.ER_MASTER_FATAL_ERROR_READING_BINLOG {
					p.logger.Error("fatal canal error", log.Error(mErr))
					return xerrors.Errorf("fatal canal error: %w", abstract.NewFatalError(err))
				}
			}
			if p.stopped {
				p.logger.Info("publisher was stopped, exiting...")
				return nil
			}
			p.logger.Warn("unable to start canal from position", log.Error(err))
			p.logger.Error("canal run failed", log.Error(err))
			return xerrors.Errorf("failed to run canal: %w", err)
		}
	}
	if p.flusherErr != nil {
		return xerrors.Errorf("flusher error: %w", p.flusherErr)
	}
	return nil
}

func (p *publisher) Stop() {
	p.once.Do(func() {
		close(p.stopCh)
		if err := p.handler.Close(); err != nil {
			p.logger.Error("failed to close binlog handler", log.Error(err))
		}
		p.canal.Close()
		p.storage.Close()
		p.stopped = true
	})
}

func (p *publisher) flusher() {
	defer p.Stop()
	lastPushedGTID := ""
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			if p.handler == nil || p.handler.nextPos.Name == "" {
				continue
			}
			p.handler.rw.Lock()
			lsn := CalculateLSN(p.handler.nextPos.Name, uint64(p.handler.nextPos.Pos))
			txSequence := p.handler.nextPos.Pos
			execTS := time.Now()
			gtidStr := fmt.Sprintf("%v", txSequence)
			if !p.gtidReplication {
				// for none gtid mode we should emit tx-like position id
				p.logger.Infof("tx done: %v %v", lastPushedGTID, p.gtidReplication)
				p.handler.inflight = append(p.handler.inflight, abstract.MakeTxDone(txSequence, lsn, execTS, lastPushedGTID, gtidStr))
				p.handler.metrics.ChangeItems.Inc()
				lastPushedGTID = gtidStr
				p.handler.rw.Unlock()
				continue
			}
			gtid := p.handler.canal.SyncedGTIDSet()
			if gtid == nil {
				// no synced gtid, nothing to force flush
				p.handler.rw.Unlock()
				continue
			}
			if gtid.String() != lastPushedGTID && len(p.handler.inflight) == 0 {
				execTS := time.Now()
				hash := fnv.New32()
				_, _ = hash.Write([]byte(gtid.String()))
				txSequence = hash.Sum32()
				gtidStr = gtid.String()
				p.logger.Infof("tx done: %v | %v | %v", gtid.String(), lastPushedGTID, len(p.handler.inflight))
				p.handler.inflight = append(p.handler.inflight, abstract.MakeTxDone(txSequence, lsn, execTS, lastPushedGTID, gtidStr))
				p.handler.metrics.ChangeItems.Inc()
				p.logger.Infof("gtid updated, need to flush")
				lastPushedGTID = gtidStr
			}
			p.handler.rw.Unlock()
		default:
		}
		h := p.handler
		h.metrics.Master.Set(1)
		if len(h.inflight) == 0 {
			time.Sleep(h.config.ReplicationFlushInterval)
			continue
		}
		start := time.Now()
		h.rw.Lock()
		infl := h.inflight
		h.inflight = make([]abstract.ChangeItem, 0)
		h.inflightSize = 0
		nPos := h.nextPos
		gtid := h.canal.SyncedGTIDSet()
		h.rw.Unlock()
		h.logger.Infof("push chunk total %v (gtid %v -> %v) next pos: %v", len(infl), infl[0].TxID, infl[len(infl)-1].TxID, h.nextPos.String())
		if err := backoff.Retry(func() error {
			if err := <-h.sink.AsyncPush(infl); err != nil {
				h.logger.Warn("unable to push", log.Error(err))
				if abstract.IsFatal(err) {
					return backoff.Permanent(err)
				}
				return xerrors.Errorf("push async faile: %w", err)
			}
			return nil
		}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)); err != nil {
			p.logger.Error("Unable to sink", log.Error(err))
			p.flusherErr = err
			return
		}
		h.metrics.PushTime.RecordDuration(time.Since(start))
		if err := backoff.Retry(func() error {
			if gtid != nil {
				if err := h.tracker.StoreGtidset(gtid); err != nil {
					h.logger.Warn("unable to store gtidset progress", log.Error(err))
					return xerrors.Errorf("unable to store gtidset: %w", err)
				}
			} else {
				if err := h.tracker.Store(nPos.Name, uint64(nPos.Pos)); err != nil {
					h.logger.Warn("unable to store progress", log.Error(err))
					return xerrors.Errorf("unable to store binlog position: %w", err)
				}
			}

			return nil
		}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 5)); err != nil {
			p.logger.Error("Unable to commit offset", log.Error(err))
			return
		}
		lastPushedGTID = infl[len(infl)-1].TxID
	}
}

func NewSource(src *MysqlSource, transferID string, objects *model.DataObjects, logger log.Logger, registry metrics.Registry, cp coordinator.Coordinator, failOnDecimal bool) (abstract.Source, error) {
	var rollbacks util.Rollbacks
	defer rollbacks.Do()

	connectionParams, err := NewConnectionParams(src.ToStorageParams())
	if err != nil {
		return nil, xerrors.Errorf("failed to create connection params: %w", err)
	}

	storage, err := NewStorage(src.ToStorageParams())
	if err != nil {
		return nil, xerrors.Errorf("failed to create storage: %w", err)
	}
	rollbacks.Add(storage.Close)

	flavor, _, err := CheckMySQLVersion(storage)
	if err != nil {
		return nil, xerrors.Errorf("unable to check MySQL version: %w", err)
	}

	config := new(Config)

	// default settings
	config.Charset = "utf8"
	config.Flavor = flavor
	config.HeartbeatPeriod = 200 * time.Millisecond
	config.ReadTimeout = 3 * time.Second
	config.UseDecimal = true // Avoid losing precision when transferring MySQL decimal values: TM-1197
	config.MaxReconnectAttempts = 5

	// user settings
	config.Addr = fmt.Sprintf("%v:%v", connectionParams.Host, connectionParams.Port)
	config.User = connectionParams.User
	config.Password = connectionParams.Password
	config.TimestampStringLocation = connectionParams.Location
	config.ServerID = src.ServerID
	config.FailOnDecimal = failOnDecimal
	includeObjects, err := abstract.BuildIncludeMap(objects.GetIncludeObjects())
	if err != nil {
		return nil, abstract.NewFatalError(xerrors.Errorf("to build exclude map: %w", err))
	}
	config.Include = func(db, table string) bool {
		if table == "__tm_keeper" || table == "__tm_gtid_keeper" {
			return false
		}
		tid := abstract.TableID{Namespace: db, Name: table}
		ok := src.Include(tid)
		if !ok {
			return false
		}

		if len(includeObjects) > 0 {
			return ok && includeObjects[tid]
		}
		return ok
	}

	if connectionParams.TLS {
		certPool, err := CreateCertPool(connectionParams.CertPEMFile, connectionParams.RootCAFiles)
		if err != nil {
			return nil, xerrors.Errorf("failed to configure TLS: %w", err)
		}
		tlsConfig := new(tls.Config)
		tlsConfig.InsecureSkipVerify = true
		tlsConfig.RootCAs = certPool
		config.TLSConfig = tlsConfig
	}

	canal, err := NewCanal(config)
	if err != nil {
		return nil, xerrors.Errorf("failed to create canal: %w", err)
	}
	rollbacks.Add(func() { canal.Close() })

	tr, err := NewTracker(src, transferID, cp)
	if err != nil {
		return nil, xerrors.Errorf("failed to create tracker: %w", err)
	}
	handler := &binlogHandler{
		inflight:         make([]abstract.ChangeItem, 0),
		logger:           logger,
		metrics:          stats.NewSourceStats(registry),
		sink:             nil,
		inflightSize:     0,
		bufferLimit:      src.BufferLimit,
		rw:               sync.Mutex{},
		config:           src,
		connectionParams: connectionParams,
		tracker:          tr,
		hasSystemDDL:     false,
		nextPos:          mysql.Position{},
		expressions:      nil,
		requiredCols:     map[string]map[string]bool{},
		canal:            canal,
		isClosed:         false,
	}
	if src.BufferLimit > 0 {
		handler.bufferLimit = src.BufferLimit
	}
	rollbacks.Add(func() {
		if err := handler.Close(); err != nil {
			logger.Error("failed to close binlog handler", log.Error(err))
		}
	})

	gtidReplication, err := IsGtidModeEnabled(storage, flavor)
	if err != nil {
		return nil, xerrors.Errorf("Unable to check gtid mode: %w", err)
	}

	canal.SetEventHandler(handler)
	p := publisher{
		logger:          logger,
		metrics:         registry,
		stopCh:          make(chan bool),
		stopped:         false,
		once:            sync.Once{},
		canal:           canal,
		handler:         handler,
		tracker:         tr,
		config:          src,
		transferID:      transferID,
		gtidReplication: gtidReplication,
		cp:              cp,
		objects:         objects,
		flusherErr:      nil,
		storage:         storage,
	}
	go p.flusher()

	rollbacks.Cancel()
	return &p, nil
}
