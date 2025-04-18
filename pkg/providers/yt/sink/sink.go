package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/core/xerrors/multierr"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/maplock"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/parsers/generic"
	yt2 "github.com/transferia/transferia/pkg/providers/yt"
	ytclient "github.com/transferia/transferia/pkg/providers/yt/client"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
	"go.ytsaurus.tech/yt/go/ytlock"
	"golang.org/x/exp/maps"
)

var MaxRetriesCount uint64 = 10 // For tests only

type GenericTable interface {
	Write(input []abstract.ChangeItem) error
}

type sinker struct {
	ytClient            yt.Client
	dir                 ypath.Path
	logger              log.Logger
	metrics             *stats.SinkerStats
	cp                  coordinator.Coordinator
	schemas             map[string][]abstract.ColSchema
	tables              map[string]GenericTable
	tableRotations      map[string]map[string]struct{}
	config              yt2.YtDestinationModel
	timeout             time.Duration
	lock                *maplock.Mutex
	chunkSize           int
	closed              bool
	progressInited      bool
	staticSnapshotState map[string]StaticTableSnapshotState // it's like map: tableName -> in-place finite-state machine

	schemaMutex        sync.Mutex
	tableRotationMutex sync.Mutex
	lsns               map[string]TableProgress
	lsnsLock           sync.Mutex
	transferID         string
	jobIndex           int
	pathToBinary       ypath.Path
}

func (s *sinker) Move(ctx context.Context, src, dst abstract.TableID) error {
	srcPath := yt2.SafeChild(s.dir, s.makeTableName(src))
	err := yt2.UnmountAndWaitRecursive(ctx, s.logger, s.ytClient, srcPath, nil)
	if err != nil {
		return xerrors.Errorf("unable to unmount source: %w", err)
	}

	dstPath := yt2.SafeChild(s.dir, s.makeTableName(dst))
	dstExists, err := s.ytClient.NodeExists(ctx, dstPath, nil)
	if err != nil {
		return xerrors.Errorf("unable to check if destination exists: %w", err)
	}
	if dstExists {
		err = yt2.UnmountAndWaitRecursive(ctx, s.logger, s.ytClient, dstPath, nil)
		if err != nil {
			return xerrors.Errorf("unable to unmount destination: %w", err)
		}
	}

	moveOptions := yt2.ResolveMoveOptions(s.ytClient, srcPath, false)
	_, err = s.ytClient.MoveNode(ctx, srcPath, dstPath, moveOptions)
	if err != nil {
		return xerrors.Errorf("unable to move: %w", err)
	}

	err = yt2.MountAndWaitRecursive(ctx, s.logger, s.ytClient, dstPath, nil)
	if err != nil {
		return xerrors.Errorf("unable to mount destination: %w", err)
	}

	return err
}

type BanRow struct {
	Cluster  string    `yson:"cluster,key"`
	Path     string    `yson:"part,key"`
	Table    string    `yson:"table,key"`
	LastFail time.Time `yson:"last_fail"`
}

type StaticTableSnapshotState string // it's like in-place finite-state machine

const (
	StaticTableSnapshotUninitialized StaticTableSnapshotState = ""
	StaticTableSnapshotInitialized   StaticTableSnapshotState = "StaticTableSnapshotInitialized" // after 'init_table_load'
	StaticTableSnapshotActivated     StaticTableSnapshotState = "StaticTableSnapshotActivated"   // 'activation' - it's successfully called checkTable
	StaticTableSnapshotDone          StaticTableSnapshotState = "StaticTableSnapshotDone"        // set in 'commitSnapshot' (on 'done_load_table'), if state is activated
)

type TableStatus string

const (
	Snapshot = TableStatus("snapshot")
	SyncWait = TableStatus("sync_wait")
	Synced   = TableStatus("synced")
)

type TableProgress struct {
	TransferID string      `yson:"transfer_id,key"`
	Table      string      `yson:"table,key"`
	LSN        uint64      `yson:"lsn"`
	Status     TableStatus `yson:"status"`
}

var (
	reTypeMismatch          = regexp.MustCompile(`Type mismatch for column .*`)
	reSortOrderMismatch     = regexp.MustCompile(`Sort order mismatch for column .*`)
	reExpressionMismatch    = regexp.MustCompile(`Expression mismatch for column .*`)
	reAggregateModeMismatch = regexp.MustCompile(`Aggregate mode mismatch for column .*`)
	reLockMismatch          = regexp.MustCompile(`Lock mismatch for key column .*`)
	reRemoveCol             = regexp.MustCompile(`Cannot remove column .*`)
	reChangeOrder           = regexp.MustCompile(`Cannot change position of a key column .*`)
	reNonKeyComputed        = regexp.MustCompile(`Non-key column .*`)
)

var schemaMismatchRes = []*regexp.Regexp{
	reTypeMismatch,
	reSortOrderMismatch,
	reExpressionMismatch,
	reAggregateModeMismatch,
	reLockMismatch,
	reRemoveCol,
	reChangeOrder,
	reNonKeyComputed,
}

var (
	TableProgressSchema = schema.MustInfer(new(TableProgress))
)

func (s *sinker) pushWalSlice(input []abstract.ChangeItem) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.config.WriteTimeoutSec())*time.Second)
	defer cancel()

	tx, rollbacks, err := beginTabletTransaction(ctx, s.ytClient, s.config.Atomicity() == yt.AtomicityFull, s.logger)
	if err != nil {
		return xerrors.Errorf("Unable to beginTabletTransaction: %w", err)
	}
	defer rollbacks.Do()
	rawWal := make([]interface{}, len(input))
	for idx, elem := range input {
		rawWal[idx] = elem
	}
	if err := tx.InsertRows(ctx, yt2.SafeChild(s.dir, yt2.TableWAL), rawWal, nil); err != nil {
		//nolint:descriptiveerrors
		return err
	}
	err = tx.Commit()
	if err != nil {
		//nolint:descriptiveerrors
		return err
	}
	rollbacks.Cancel()
	return nil
}

func (s *sinker) pushWal(input []abstract.ChangeItem) error {
	if err := s.checkTable(WalTableSchema, yt2.TableWAL); err != nil {
		//nolint:descriptiveerrors
		return err
	}
	for i := 0; i < len(input); i += s.chunkSize {
		end := i + s.chunkSize

		if end > len(input) {
			end = len(input)
		}

		s.logger.Info("Write wal", log.Any("size", len(input[i:end])))
		if err := s.pushWalSlice(input[i:end]); err != nil {
			//nolint:descriptiveerrors
			return err
		}
	}
	s.metrics.Wal.Add(int64(len(input)))
	return nil
}

func (s *sinker) Close() error {
	s.closed = true
	var errs error

	for _, table := range s.tables {
		if sst, ok := table.(*SingleStaticTable); ok {
			errs = multierr.Append(errs, sst.Abort())
		}
	}
	return errs
}

func (s *sinker) checkTable(schema []abstract.ColSchema, table string) error {
	for {
		if s.lock.TryLock(table) {
			break
		}
		time.Sleep(time.Second)
		s.logger.Debugf("Unable to lock table checker %v", table)
	}
	defer s.lock.Unlock(table)

	if s.staticSnapshotState[table] == StaticTableSnapshotInitialized {
		s.logger.Info("Try to create snapshot table", log.Any("table", table), log.Any("schema", schema))
		// special case for InitTableLoad
		var err error
		currStaticTable, err := s.newStaticTable(schema, table)
		if err != nil {
			return xerrors.Errorf("cannot create static table for pre-store in YT for snapshot upload, table: %s, err: %v", table, err)
		}
		s.tables[table] = currStaticTable
		s.staticSnapshotState[table] = StaticTableSnapshotActivated
		return nil
	}

	_, isSst := s.tables[table].(*SingleStaticTable)
	if isSst {
		if s.staticSnapshotState[table] == StaticTableSnapshotDone {
			// recreate writer on snapshot completion
			s.tables[table] = nil
		}
	}

	if s.tables[table] == nil {
		if schema == nil {
			s.logger.Error("No schema for table", log.Any("table", table))
			return xerrors.New("no schema for table")
		}

		s.logger.Info("Try to create table", log.Any("table", table), log.Any("schema", schema))
		genericTable, createTableErr := s.newGenericTable(schema, table)
		if createTableErr != nil {
			s.logger.Error("Create table error", log.Any("table", table), log.Error(createTableErr))
			if isIncompatibleSchema(createTableErr) {
				return xerrors.Errorf("incompatible schema changes in table %s: %w", table, createTableErr)
			}
			return xerrors.Errorf("failed to create table in YT: %w", createTableErr)
		}
		s.logger.Info("Table created", log.Any("table", table), log.Any("schema", schema))
		s.tables[table] = genericTable
	}

	return nil
}

func isIncompatibleSchema(err error) bool {
	if IsIncompatibleSchemaErr(err) {
		return true
	}
	for _, re := range schemaMismatchRes {
		if yterrors.ContainsMessageRE(err, re) {
			return true
		}
	}
	return false
}

func (s *sinker) checkPrimaryKeyChanges(input []abstract.ChangeItem) error {
	if !s.config.Ordered() && s.config.VersionColumn() == "" {
		// Sorted tables support handle primary key changes, but the others don't. TM-1143
		return nil
	}

	if changeitem.InsertsOnly(input) {
		return nil
	}
	start := time.Now()
	for i := range input {
		item := &input[i]
		if item.KeysChanged() {
			serializedItem, _ := json.Marshal(item)
			if s.config.TolerateKeyChanges() {
				s.logger.Warn("Primary key change event detected. These events are not yet supported, sink may contain extra rows", log.String("change", string(serializedItem)))
			} else {
				return xerrors.Errorf("Primary key changes are not supported for YT target; change: %s", string(serializedItem))
			}
		}
	}
	elapsed := time.Since(start)
	s.metrics.RecordDuration("pkeychange.total", elapsed)
	s.metrics.RecordDuration("pkeychange.average", elapsed/time.Duration(len(input)))
	return nil
}

func (s *sinker) makeTableName(tableID abstract.TableID) string {
	var name string
	if tableID.Namespace == "public" || tableID.Namespace == "" {
		name = tableID.Name
	} else {
		name = fmt.Sprintf("%v_%v", tableID.Namespace, tableID.Name)
	}

	if altName, ok := s.config.AltNames()[name]; ok {
		name = altName
	}

	return name
}

func (s *sinker) isTableSchemaDefined(tableName string) bool {
	s.schemaMutex.Lock()
	defer s.schemaMutex.Unlock()
	return len(s.schemas[tableName]) > 0
}

func (s *sinker) addTableRotation(tableName string, rotationName string) {
	s.tableRotationMutex.Lock()
	defer s.tableRotationMutex.Unlock()
	if _, ok := s.tableRotations[tableName]; !ok {
		s.tableRotations[tableName] = make(map[string]struct{})
	}
	s.tableRotations[tableName][rotationName] = struct{}{}
}

func (s *sinker) getTableRotations(name string) map[string]struct{} {
	s.tableRotationMutex.Lock()
	defer s.tableRotationMutex.Unlock()
	return s.tableRotations[name]
}

// Push processes incoming items.
//
// WARNING: All non-row items must be pushed in a DISTINCT CALL to this function - separately from row items.
func (s *sinker) Push(input []abstract.ChangeItem) error {
	start := time.Now()
	rotationBatches := map[string][]abstract.ChangeItem{}
	if s.config.PushWal() {
		if err := s.pushWal(input); err != nil {
			return xerrors.Errorf("unable to push WAL: %w", err)
		}
	}

	if err := s.checkPrimaryKeyChanges(input); err != nil {
		return xerrors.Errorf("unable to check primary key changes: %w", err)
	}
	progress, err := s.loadTableStatus()
	if err != nil {
		return xerrors.Errorf("unable to load table status: %w", err)
	}
	syncQ := map[string]uint64{}
	for _, item := range input {
		name := s.makeTableName(item.TableID())
		tableYPath := yt2.SafeChild(s.dir, name)
		if !s.isTableSchemaDefined(name) {
			s.schemaMutex.Lock()
			s.schemas[name] = item.TableSchema.Columns()
			s.schemaMutex.Unlock()
		}
		// apply rotation to name
		rotatedName := s.config.Rotation().AnnotateWithTimeFromColumn(name, item)
		s.addTableRotation(name, rotatedName)

		switch item.Kind {
		// Drop and truncate are essentially the same operations
		case abstract.DropTableKind, abstract.TruncateTableKind:
			if s.config.CleanupMode() == model.DisabledCleanup {
				s.logger.Infof("Skipped dropping/truncating table '%v' due cleanup policy", tableYPath)
				continue
			}
			// note: does not affect rotated tables
			exists, err := s.ytClient.NodeExists(context.Background(), tableYPath, nil)
			if err != nil {
				return xerrors.Errorf("Unable to check path %v for existence: %w", tableYPath, err)
			}
			if !exists {
				continue
			}
			if err := migrate.UnmountAndWait(context.Background(), s.ytClient, tableYPath); err != nil {
				s.logger.Warn("unable to unmount path", log.Any("path", tableYPath), log.Error(err))
			}
			if err := s.ytClient.RemoveNode(context.Background(), tableYPath, &yt.RemoveNodeOptions{
				Recursive: s.config.Rotation() != nil, // for rotation tables child is a directory with sub nodes see DTSUPPORT-852
				Force:     true,
			}); err != nil {
				return xerrors.Errorf("unable to remove node: %w", err)
			}
		case abstract.InitShardedTableLoad:
			if err := s.setTableStatus(item.Fqtn(), item.LSN, Snapshot); err != nil {
				return xerrors.Errorf("unable to set status %q for table %v: %w", Snapshot, item.Fqtn(), err)
			}
			progress, err = s.loadTableStatus()
			if err != nil {
				return xerrors.Errorf("unable to load status for table %v: %w", item.Fqtn(), err)
			}
		case abstract.InitTableLoad:
			if s.staticSnapshotState[rotatedName] != StaticTableSnapshotUninitialized {
				msg := "attempt to initialize snapshot twice in a row"
				s.logger.Error(msg, log.String("staticSnapshotState", string(s.staticSnapshotState[rotatedName])))
				return abstract.NewFatalError(xerrors.Errorf(msg))
			}
			// if static snapshots are enabled
			if s.config.UseStaticTableOnSnapshot() {
				s.logger.Info("static table snapshot is initiated because policy UseStaticTableOnSnapshot is on")
				// delay handler application until schema is unknown
				s.staticSnapshotState[rotatedName] = StaticTableSnapshotInitialized

				if s.config.CreateEmptyTables() {
					s.logger.Info("create empty table", log.String("name", name))
					if err := s.checkTable(item.TableSchema.Columns(), name); err != nil {
						return xerrors.Errorf("unable to create table on init table load (CreateEmptyTables is true): %w", err)
					}
				}
			} else {
				s.logger.Info("static table snapshot is not initiated because policy UseStaticTableOnSnapshot is off")
			}
		case abstract.DoneTableLoad:
			if s.config.UseStaticTableOnSnapshot() {
				err := s.commitSnapshot(name)
				if err != nil {
					return xerrors.Errorf("unable to commit snapshot: %w", err)
				}
			}
		case abstract.DoneShardedTableLoad:
			if s.config.UseStaticTableOnSnapshot() {
				var tableWriterSpec interface{}
				if spec := s.config.Spec(); spec != nil {
					tableWriterSpec = spec.GetConfig()
				}
				err := finishSingleStaticTableLoading(context.TODO(), s.ytClient, s.logger, s.dir, s.transferID, name, s.config.CleanupMode(), s.pathToBinary, tableWriterSpec,
					func(schema schema.Schema) map[string]interface{} {
						return buildDynamicAttrs(getCols(schema), s.config)
					}, s.config.Rotation() != nil)
				if err != nil {
					return xerrors.Errorf("unable to finish loading table %q: %w", name, err)
				}
			}

			if err := s.setTableStatus(item.Fqtn(), item.LSN, SyncWait); err != nil {
				return xerrors.Errorf("unable to set status %q for table %v: %w", SyncWait, item.Fqtn(), err)
			}
			progress, err = s.loadTableStatus()
			if err != nil {
				return xerrors.Errorf("unable to load status for table %v: %w", item.Fqtn(), err)
			}
		case abstract.InsertKind, abstract.UpdateKind, abstract.DeleteKind:
			if progress[item.Fqtn()].Status == SyncWait {
				if item.LSN < progress[item.Fqtn()].LSN {
					s.logger.Infof("skip row due to sync wait: %v, Source(%v) -> Synced(%v)", progress[item.Fqtn()].Table, item.LSN, progress[item.Fqtn()].LSN)
					continue
				} else {
					if _, ok := syncQ[item.Fqtn()]; !ok {
						syncQ[item.Fqtn()] = item.LSN
					}
				}
			}
			rotationBatches[rotatedName] = append(rotationBatches[rotatedName], item)
		default:
			s.logger.Infof("kind: %v not supported", item.Kind)
		}
	}

	if err := s.pushBatchesParallel(rotationBatches); err != nil {
		return xerrors.Errorf("unable to push batches: %w", err)
	}

	for table, lsn := range syncQ {
		s.logger.Infof("table: %v is synced at LSN %v", table, lsn)
		if err := s.setTableStatus(table, lsn, Synced); err != nil {
			return xerrors.Errorf("unable to set table (%v) status: %w", table, err)
		}
	}

	s.metrics.Elapsed.RecordDuration(time.Since(start))
	return nil
}

const parallelism = 10

func (s *sinker) pushBatchesParallel(rotationBatches map[string][]abstract.ChangeItem) error {
	tables := maps.Keys(rotationBatches)
	return util.ParallelDo(context.Background(), len(rotationBatches), parallelism, func(i int) error {
		table := tables[i]
		return s.pushOneBatch(table, rotationBatches[table])
	})
}

func (s *sinker) pushOneBatch(table string, batch []abstract.ChangeItem) error {
	start := time.Now()

	s.logger.Debugf("table: %v, len(batch): %v", table, len(batch))

	if len(table) == 0 || table[len(table)-1:] == "/" {
		s.logger.Warnf("Bad table name, skip")
		return nil
	}

	var scm []abstract.ColSchema
	for _, e := range batch {
		if len(e.TableSchema.Columns()) > 0 {
			scm = e.TableSchema.Columns()
			break
		}
	}

	if len(scm) == 0 && s.tables[table] == nil {
		if !s.config.LoseDataOnError() {
			return xerrors.Errorf("unable to find schema for %v", table)
		}
	}

	if err := s.checkTable(scm, table); err != nil {
		s.logger.Error("Check table error", log.Error(err), log.Any("table", table))
		return xerrors.Errorf("Check table (%v) error: %w", table, err)
	}

	if changeitem.InsertsOnly(batch) {
		if err := s.pushSlice(batch, table); err != nil {
			return xerrors.Errorf("unable to upload batch: %w", err)
		}
		s.logger.Infof("Upload %v changes delay %v", len(batch), time.Since(start))
	} else {
		// YT have a-b-a problem with PKey update, this would split such changes in sub-batches without PKey updates.
		for _, subslice := range abstract.SplitUpdatedPKeys(batch) {
			if err := s.processPKUpdates(subslice, table); err != nil {
				return xerrors.Errorf("failed while processing key update: %w", err)
			}
			if err := s.pushSlice(subslice, table); err != nil {
				return xerrors.Errorf("unable to upload batch: %w", err)
			}
			s.logger.Infof("Upload %v changes delay %v", len(subslice), time.Since(start))
		}
	}
	return nil
}

// if we catch change with primary keys update we will transform it to insert + delete
// When processing insert we will add __dummy column, if only primary keys were present. This will lead to error
// If some non PK colum were absent in update we will lose data
// Therefore we first try to fill this updates with non primary key col values
func (s *sinker) processPKUpdates(batch []abstract.ChangeItem, table string) error {
	if len(batch) != 2 {
		return nil
	}
	if batch[1].Kind != abstract.InsertKind || batch[0].Kind != abstract.DeleteKind {
		return nil
	}
	if len(batch[1].TableSchema.Columns()) == len(batch[1].ColumnNames) { // All columns are present in change
		return nil
	}

	deleteItem := batch[0]
	insertItem := batch[1]
	keys := ytRow{}
	columns := newTableColumns(deleteItem.TableSchema.Columns())
	for i, colName := range deleteItem.OldKeys.KeyNames {
		colSchema, ok := columns.getByName(colName)
		if !ok {
			return xerrors.Errorf("Cannot find column %s in schema %v", colName, columns)
		}
		if colSchema.PrimaryKey {
			var err error
			keys[colName], err = Restore(colSchema, deleteItem.OldKeys.KeyValues[i])
			if err != nil {
				return xerrors.Errorf("Cannot restore value for column '%s': %w", colName, err)
			}
		}
	}
	if len(keys) == 0 {
		return xerrors.Errorf("No old key columns found for change item %s", util.Sample(deleteItem.ToJSONString(), 10000))
	}

	tablePath := yt2.SafeChild(s.dir, table)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := backoff.Retry(func() error {
		reader, err := s.ytClient.LookupRows(ctx, tablePath, []any{keys}, &yt.LookupRowsOptions{})
		if err != nil {
			return xerrors.Errorf("Cannot lookup row: %w", err)
		}
		defer reader.Close()

		if !reader.Next() {
			return xerrors.Errorf("Trying to update row that does not exist, possible err: %v", reader.Err())
		}
		var oldRow ytRow
		if err := reader.Scan(&oldRow); err != nil {
			return xerrors.Errorf("Cannot scan value: %w", err)
		}

		newColValues := make([]any, len(insertItem.TableSchema.ColumnNames()))
		for idx, colName := range insertItem.ColumnNames {
			oldRow[colName] = insertItem.ColumnValues[idx]
		}
		for idx, colName := range insertItem.TableSchema.ColumnNames() {
			newColValues[idx] = oldRow[colName]
		}
		insertItem.ColumnNames = insertItem.TableSchema.ColumnNames()
		insertItem.ColumnValues = newColValues
		batch[1] = insertItem

		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 5)); err != nil {
		return xerrors.Errorf("unable to do lookup row for primary key update: %w", err)
	}
	return nil
}

func lastCommitTime(chunk []abstract.ChangeItem) time.Time {
	if len(chunk) == 0 {
		return time.Unix(0, 0)
	}
	commitTimeNsec := chunk[len(chunk)-1].CommitTime
	return time.Unix(0, int64(commitTimeNsec))
}

func (s *sinker) pushSlice(batch []abstract.ChangeItem, table string) error {
	iterations := int(math.Ceil(float64(len(batch)) / float64(s.chunkSize)))
	for i := 0; i < len(batch); i += s.chunkSize {
		end := i + s.chunkSize

		if end > len(batch) {
			end = len(batch)
		}

		start := time.Now()

		s.metrics.Inflight.Inc()
		if err := backoff.Retry(func() error {
			chunk := batch[i:end]
			err := s.tables[table].Write(chunk)
			if err != nil {
				s.logger.Warn(
					fmt.Sprintf("Write returned error. i: %v, iterations: %v, err: %v", i, iterations, err),
					log.Any("table", table),
					log.Error(err),
				)
				if strings.Contains(err.Error(), "value is too long:") && s.config.LoseDataOnError() {
					s.logger.Warn("Value is too big", log.Error(err))
				} else if s.config.DiscardBigValues() &&
					(strings.Contains(err.Error(), "is too long for dynamic data") ||
						strings.Contains(err.Error(), "memory limit exceeded while parsing YSON stream: allocated")) {
					s.logger.Warn("batch was discarded", log.Error(err))
					for _, item := range chunk {
						keys := item.KeysAsMap()
						s.logger.Warn("change item was discarded", log.String("table", item.Fqtn()), log.Any("keys", keys))
					}
				} else {
					return err
				}
			}
			s.logger.Info(
				"Committed",
				log.Any("table", table),
				log.Any("delay", time.Since(lastCommitTime(chunk))),
				log.Any("elapsed", time.Since(start)),
				log.Any("ops", len(batch[i:end])),
			)
			return nil
		}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), MaxRetriesCount)); err != nil {
			s.logger.Warn(
				fmt.Sprintf("Write returned error, backoff-Retry didnt help. i: %v, iterations: %v, err: %v", i, iterations, err),
				log.Any("table", table),
				log.Error(err),
			)
			s.metrics.Table(table, "error", 1)
			return err
		}
		s.metrics.Table(table, "rows", len(batch[i:end]))
	}
	return nil
}

type YtRotationNode struct {
	Name string `yson:",value"`
	Type string `yson:"type,attr"`
	Path string `yson:"path,attr"`
}

func (s *sinker) rotateTable() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*8)
	defer cancel()

	baseTime := s.config.Rotation().BaseTime()
	s.logger.Info("Initiate rotation with base time", log.Time("baseTime", baseTime))

	ytListNodeOptions := &yt.ListNodeOptions{Attributes: []string{"type", "path"}}
	deletedCount := 0
	unknownCount := 0
	skipCount := 0

	tableNames := []string{}
	s.schemaMutex.Lock()
	for tableName := range s.schemas {
		tableNames = append(tableNames, tableName)
	}
	s.schemaMutex.Unlock()
	for _, tableName := range tableNames {
		nodePath := yt2.SafeChild(s.dir, tableName)
		var childNodes []YtRotationNode
		if err := s.ytClient.ListNode(ctx, nodePath, &childNodes, ytListNodeOptions); err != nil {
			return err
		}

		for _, childNode := range childNodes {
			if childNode.Type != "table" {
				continue
			}

			tableTime, err := s.config.Rotation().ParseTime(childNode.Name)
			if err != nil {
				skipCount++
				continue
			}
			if tableTime.Before(baseTime) {
				var currentState string
				path := ypath.Path(childNode.Path)
				err := s.ytClient.GetNode(ctx, path.Attr("tablet_state"), &currentState, nil)
				if err != nil {
					s.logger.Warnf("Error while getting tablet_state of table '%s': %s", path, err.Error())
					continue
				}
				if currentState != yt.TabletMounted {
					s.logger.Warnf("tablet_state of path '%s' is not mounted. skipping", path)
					continue
				}

				s.logger.Infof("Delete old table '%v'", path)
				if err := migrate.UnmountAndWait(ctx, s.ytClient, path); err != nil {
					return xerrors.Errorf("unable to unmount table: %w", err)
				}
				if err := s.ytClient.RemoveNode(ctx, path, nil); err != nil {
					return xerrors.Errorf("unable to remove node: %w", err)
				}
				deletedCount++
			}

			tablePath := s.config.Rotation().Next(tableName)
			if !s.isTableSchemaDefined(tableName) {
				s.logger.Warnf("Unable to init clone of %v: no schema in cache", tableName)
				continue
			}
			s.schemaMutex.Lock()
			tSchema := s.schemas[tableName]
			s.schemaMutex.Unlock()
			if err := s.checkTable(tSchema, tablePath); err != nil {
				s.logger.Warn("Unable to init clone", log.Error(err))
			}
		}
	}

	s.logger.Info("Deleted rotation table statistics",
		log.Int("deletedCount", deletedCount), log.Int("unknownCount", unknownCount), log.Int("skipCount", skipCount))

	return nil
}

func (s *sinker) runRotator() {
	defer s.Close()
	defer s.logger.Info("Rotation goroutine stopped")
	s.logger.Info("Rotation goroutine started")
	for {
		if s.closed {
			return
		}

		lock := ytlock.NewLock(s.ytClient, yt2.SafeChild(s.dir, "__lock"))
		_, err := lock.Acquire(context.Background())
		if err != nil {
			s.logger.Debug("unable to lock", log.Error(err))
			time.Sleep(10 * time.Minute)
			continue
		}
		cleanup := func() {
			err := lock.Release(context.Background())
			if err != nil {
				s.logger.Warn("unable to release", log.Error(err))
			}
		}
		if err := s.rotateTable(); err != nil {
			s.logger.Warn("runRotator err", log.Error(err))
		}
		cleanup()
		time.Sleep(2 * time.Minute)
	}
}

func (s *sinker) setTableStatus(fqtn string, lsn uint64, status TableStatus) error {
	tableProgressPath := yt2.SafeChild(s.dir, yt2.TableProgressRelativePath)
	s.lsnsLock.Lock()
	defer s.lsnsLock.Unlock()
	if !s.progressInited {
		tables := map[ypath.Path]migrate.Table{}
		tables[tableProgressPath] = migrate.Table{
			Schema: TableProgressSchema,
			Attributes: map[string]any{
				"tablet_cell_bundle": s.config.CellBundle(),
			},
		}
		drop := migrate.OnConflictDrop(context.Background(), s.ytClient)
		if err := migrate.EnsureTables(context.Background(), s.ytClient, tables, drop); err != nil {
			s.logger.Warn("Unable to init progress table", log.Error(err))
			return err
		}
		s.progressInited = true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := backoff.Retry(func() error {
		err := s.ytClient.InsertRows(ctx, tableProgressPath, []interface{}{TableProgress{
			Table:      fqtn,
			TransferID: s.transferID,
			LSN:        lsn,
			Status:     status,
		}}, nil)
		if err != nil {
			s.logger.Warnf("Cannot update table(%v) status(%v): %v", fqtn, status, err)
		}
		return err
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 5))
	if err != nil {
		s.logger.Errorf("Unable to update table(%v) status(%v) after several retries: %v", fqtn, status, err)
		return xerrors.Errorf("Unable to update table(%v) status(%v) after several retries: %w", fqtn, status, err)
	}
	s.lsns = nil
	return nil
}

func (s *sinker) loadTableStatus() (map[string]TableProgress, error) {
	tableProgressPath := yt2.SafeChild(s.dir, yt2.TableProgressRelativePath)
	s.lsnsLock.Lock()
	defer s.lsnsLock.Unlock()
	if s.lsns != nil {
		// if no progress table then should do nothing
		return s.lsns, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s.lsns = map[string]TableProgress{}
	exists, err := s.ytClient.NodeExists(ctx, tableProgressPath, nil)
	if err != nil {
		return s.lsns, xerrors.Errorf("Unable check exists %v path: %w", tableProgressPath, err)
	}
	if !exists {
		return s.lsns, nil
	}
	if err = yt2.WaitMountingPreloadState(s.ytClient, tableProgressPath); err != nil {
		return nil, xerrors.Errorf("table is not mounted for reading: %w", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := s.ytClient.SelectRows(
		ctx,
		fmt.Sprintf("* from [%v] where transfer_id = '%v'", tableProgressPath, s.transferID),
		nil,
	)
	if err != nil {
		return s.lsns, xerrors.Errorf("Unable select %v: %w", tableProgressPath, err)
	}
	s.lsns = map[string]TableProgress{}
	for rows.Next() {
		var res TableProgress
		if err := rows.Scan(&res); err != nil {
			return s.lsns, xerrors.Errorf("Unable scan %v: %w", tableProgressPath, err)
		}
		s.lsns[res.Table] = res
	}
	return s.lsns, nil
}

func (s *sinker) commitSnapshot(tableName string) error {
	rotations := s.getTableRotations(tableName)
	for rotation := range rotations {
		// immediately convert table from static to dynamic (for code simplification)
		// so, DoneTableLoad assumed to be pushed as separate chunk of data
		if s.staticSnapshotState[rotation] == StaticTableSnapshotInitialized {
			s.logger.Info("Received snapshot completion while initialization is not yet launched. Assuming, that initialization is over.",
				log.String("table", rotation), log.Any("StaticTableSnapshotState", s.staticSnapshotState[rotation]))
			// Init and Done in the same batch, cancel initialization
			s.staticSnapshotState[rotation] = StaticTableSnapshotDone
			continue
		}
		if s.staticSnapshotState[rotation] != StaticTableSnapshotActivated {
			return xerrors.Errorf("Received snapshot completion without activation the snapshot, table:%s, StaticTableSnapshotState:%s", rotation, s.staticSnapshotState[rotation])
		}

		if sst, ok := s.tables[rotation].(*SingleStaticTable); ok {
			s.schemaMutex.Lock()
			sst.UpdateSchema(s.schemas[tableName]) // during snapshot upload we know exact schema
			s.schemaMutex.Unlock()
			ctx := context.Background()
			if err := sst.Commit(ctx); err != nil {
				s.logger.Error("Snapshot commit error",
					log.String("table", rotation),
					log.Error(err))
				//nolint:descriptiveerrors
				return err
			}
			s.logger.Info("Snapshot initialization done",
				log.String("table", rotation))
			s.staticSnapshotState[rotation] = StaticTableSnapshotDone
		} else {
			err := fmt.Errorf("static type conversion error: writer is not SingleStaticTable %v", rotation)
			s.logger.Warn("Couldn't finish snapshot: ", log.Error(err), log.String("table", rotation))
			//nolint:descriptiveerrors
			return err
		}
	}
	return nil
}

// private wrapper function with receiver
func (s *sinker) newGenericTable(schema []abstract.ColSchema, table string) (GenericTable, error) {
	return NewGenericTable(s.ytClient, yt2.SafeChild(s.dir, table), schema, s.config, s.metrics, s.logger)
}

func (s *sinker) newStaticTable(schema []abstract.ColSchema, table string) (GenericTable, error) {
	return NewSingleStaticTable(s.ytClient, s.dir, table, schema, s.config, s.jobIndex, s.transferID, s.config.CleanupMode(), s.metrics, s.logger, s.pathToBinary)
}

func (s *sinker) OverrideClient(client yt.Client) {
	s.ytClient = client
}

func NewSinker(
	cfg yt2.YtDestinationModel,
	transferID string,
	jobIndex int,
	logger log.Logger,
	registry metrics.Registry,
	cp coordinator.Coordinator,
	tmpPolicyConfig *model.TmpPolicyConfig,
) (abstract.Sinker, error) {
	var result abstract.Sinker

	uncasted, err := newSinker(cfg, transferID, jobIndex, logger, registry, cp)
	if err != nil {
		return nil, xerrors.Errorf("failed to create pure YT sink: %w", err)
	}

	if tmpPolicyConfig != nil {
		result = middlewares.TableTemporator(logger, transferID, *tmpPolicyConfig)(uncasted)
	} else {
		result = uncasted
	}

	return result, nil
}

func newSinker(cfg yt2.YtDestinationModel, transferID string, jobIndex int, lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator) (*sinker, error) {
	ytClient, err := ytclient.FromConnParams(cfg, lgr)
	if err != nil {
		return nil, xerrors.Errorf("error getting YT Client: %w", err)
	}

	chunkSize := int(cfg.ChunkSize())
	if len(cfg.Index()) > 0 {
		chunkSize = chunkSize / (len(cfg.Index()) + 1)
	}

	pathToBinary, err := dataplaneExecutablePath(cfg, ytClient, lgr)
	if err != nil {
		return nil, xerrors.Errorf("unable to get dataplane executable path: %w", err)
	}

	s := sinker{
		ytClient:            ytClient,
		dir:                 ypath.Path(cfg.Path()),
		logger:              lgr,
		metrics:             stats.NewSinkerStats(registry),
		schemas:             map[string][]abstract.ColSchema{},
		tables:              map[string]GenericTable{},
		tableRotations:      map[string]map[string]struct{}{},
		config:              cfg,
		timeout:             15 * time.Second,
		lock:                maplock.NewMapMutex(),
		chunkSize:           chunkSize,
		closed:              false,
		progressInited:      false,
		staticSnapshotState: map[string]StaticTableSnapshotState{},
		schemaMutex:         sync.Mutex{},
		tableRotationMutex:  sync.Mutex{},
		lsns:                map[string]TableProgress{},
		lsnsLock:            sync.Mutex{},
		transferID:          transferID,
		cp:                  cp,
		jobIndex:            jobIndex,
		pathToBinary:        pathToBinary,
	}

	if cfg.Rotation() != nil {
		go s.runRotator()
	}

	return &s, nil
}

func NewGenericTable(ytClient yt.Client, path ypath.Path, schema []abstract.ColSchema, cfg yt2.YtDestinationModel, metrics *stats.SinkerStats, logger log.Logger) (GenericTable, error) {
	logger.Info("create generic table", log.Any("name", path), log.Any("schema", schema))
	originalSchema := schema
	if !cfg.DisableDatetimeHack() {
		schema = hackTimestamps(schema)
		logger.Warn("nasty hack that replace datetime -> int64", log.Any("name", path), log.Any("schema", schema))
	}
	if cfg.Ordered() {
		orderedTable, err := NewOrderedTable(ytClient, path, schema, cfg, metrics, logger)
		if err != nil {
			return nil, xerrors.Errorf("cannot create ordered table: %w", err)
		}
		return orderedTable, nil
	}
	if cfg.VersionColumn() != "" {
		if generic.IsGenericUnparsedSchema(abstract.NewTableSchema(schema)) &&
			strings.HasSuffix(path.String(), "_unparsed") {
			logger.Info("Table with unparsed schema and _unparsed postfix detected, creation of versioned table is skipped",
				log.Any("table", path), log.Any("version_column", cfg.VersionColumn()),
				log.Any("schema", schema))
		} else if _, ok := abstract.MakeFastTableSchema(schema)[abstract.ColumnName(cfg.VersionColumn())]; !ok {
			return nil, abstract.NewFatalError(xerrors.Errorf(
				"config error: detected table '%v' without column specified as version column '%v'",
				path, cfg.VersionColumn()),
			)
		} else if cfg.Rotation() != nil {
			return nil, abstract.NewFatalError(xerrors.New("rotation is not supported with versioned tables"))
		} else {
			versionedTable, err := NewVersionedTable(ytClient, path, schema, cfg, metrics, logger)
			if err != nil {
				return nil, xerrors.Errorf("cannot create versioned table: %w", err)
			}
			return versionedTable, nil
		}
	}

	sortedTable, err := NewSortedTable(ytClient, path, schema, cfg, metrics, logger)
	if err != nil {
		return nil, xerrors.Errorf("cannot create sorted table: %w", err)
	}
	if !cfg.DisableDatetimeHack() {
		// this hack force agly code, if hack is enabled we rebuild EVERY change item schema, which is very costly
		sortedTable.tableSchema = abstract.NewTableSchema(originalSchema)
	}
	return sortedTable, nil
}

func dataplaneExecutablePath(cfg yt2.YtDestinationModel, ytClient yt.Client, logger log.Logger) (ypath.Path, error) {
	if dataplaneVersion, ok := yt2.DataplaneVersion(); ok {
		pathToBinary := yt2.DataplaneExecutablePath(cfg.Cluster(), dataplaneVersion)
		if exists, err := ytClient.NodeExists(context.TODO(), pathToBinary, nil); err != nil {
			return "", xerrors.Errorf("unable to check if dataplane executable exists: %w", err)
		} else if !exists {
			logger.Warn("dataplane executable path does not exist", log.Any("path", pathToBinary))
			return "", nil
		} else {
			logger.Info("successfully initialized dataplane executable path", log.Any("path", pathToBinary))
			return pathToBinary, nil
		}
	} else {
		logger.Warn("dataplane version is not specified")
		return "", nil
	}
}

func hackTimestamps(cols []abstract.ColSchema) []abstract.ColSchema {
	var res []abstract.ColSchema
	for _, col := range cols {
		res = append(res, abstract.ColSchema{
			TableSchema:  col.TableSchema,
			TableName:    col.TableName,
			Path:         col.Path,
			ColumnName:   col.ColumnName,
			DataType:     tryHackType(col),
			PrimaryKey:   col.PrimaryKey,
			FakeKey:      col.FakeKey,
			Required:     col.Required,
			Expression:   col.Expression,
			OriginalType: col.OriginalType,
			Properties:   nil,
		})
	}
	return res
}

func NewRotatedStaticSink(cfg yt2.YtDestinationModel, registry metrics.Registry, logger log.Logger, cp coordinator.Coordinator, transferID string) (abstract.Sinker, error) {
	ytClient, err := ytclient.FromConnParams(cfg, logger)
	if err != nil {
		return nil, err
	}

	t := NewStaticTableFromConfig(ytClient, cfg, registry, logger, cp, transferID)
	return t, nil
}
