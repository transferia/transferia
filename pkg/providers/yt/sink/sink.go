//go:build !disable_yt_provider

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
	ytClient       yt.Client
	dir            ypath.Path
	logger         log.Logger
	metrics        *stats.SinkerStats
	cp             coordinator.Coordinator
	schemas        map[string][]abstract.ColSchema
	tables         map[string]GenericTable
	config         yt2.YtDestinationModel
	lock           *maplock.Mutex
	chunkSize      int
	closed         bool
	progressInited bool

	schemaMutex sync.Mutex
	transferID  string
}

func (s *sinker) Move(ctx context.Context, src, dst abstract.TableID) error {
	srcPath := yt2.SafeChild(s.dir, yt2.MakeTableName(src, s.config.AltNames()))
	err := yt2.UnmountAndWaitRecursive(ctx, s.logger, s.ytClient, srcPath, nil)
	if err != nil {
		return xerrors.Errorf("unable to unmount source: %w", err)
	}

	dstPath := yt2.SafeChild(s.dir, yt2.MakeTableName(dst, s.config.AltNames()))
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
	return nil
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

	if s.tables[table] == nil {
		if schema == nil {
			s.logger.Error("No schema for table", log.Any("table", table))
			return xerrors.New("no schema for table")
		}

		s.logger.Info("Try to create table", log.Any("table", table), log.Any("schema", schema))
		genericTable, createTableErr := s.newGenericTable(yt2.SafeChild(s.dir, table), schema)
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

func (s *sinker) isTableSchemaDefined(tableName string) bool {
	s.schemaMutex.Lock()
	defer s.schemaMutex.Unlock()
	return len(s.schemas[tableName]) > 0
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

	for _, item := range input {
		name := yt2.MakeTableName(item.TableID(), s.config.AltNames())
		tableYPath := yt2.SafeChild(s.dir, name)
		if !s.isTableSchemaDefined(name) {
			s.schemaMutex.Lock()
			s.schemas[name] = item.TableSchema.Columns()
			s.schemaMutex.Unlock()
		}
		// apply rotation to name
		rotatedName := s.config.Rotation().AnnotateWithTimeFromColumn(name, item)

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
			if err := yt2.MountUnmountWrapper(context.Background(), s.ytClient, tableYPath, migrate.UnmountAndWait); err != nil {
				s.logger.Warn("unable to unmount path", log.Any("path", tableYPath), log.Error(err))
			}
			if err := s.ytClient.RemoveNode(context.Background(), tableYPath, &yt.RemoveNodeOptions{
				Recursive: s.config.Rotation() != nil, // for rotation tables child is a directory with sub nodes see DTSUPPORT-852
				Force:     true,
			}); err != nil {
				return xerrors.Errorf("unable to remove node: %w", err)
			}
		case abstract.InsertKind, abstract.UpdateKind, abstract.DeleteKind:
			rotationBatches[rotatedName] = append(rotationBatches[rotatedName], item)
		case abstract.SynchronizeKind:
			// do nothing
		default:
			s.logger.Infof("kind: %v not supported", item.Kind)
		}
	}

	if err := s.pushBatchesParallel(rotationBatches); err != nil {
		return xerrors.Errorf("unable to push batches: %w", err)
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
			keys[colName], err = RestoreWithLengthLimitCheck(colSchema, deleteItem.OldKeys.KeyValues[i], s.config.DiscardBigValues(), YtDynMaxStringLength)
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
				return err
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
				if err := yt2.MountUnmountWrapper(ctx, s.ytClient, path, migrate.UnmountAndWait); err != nil {
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

	s.logger.Info("Deleted rotation table statistics", log.Int("deletedCount", deletedCount), log.Int("skipCount", skipCount))
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

func NewSinker(
	cfg yt2.YtDestinationModel,
	transferID string,
	logger log.Logger,
	registry metrics.Registry,
	cp coordinator.Coordinator,
	tmpPolicyConfig *model.TmpPolicyConfig,
) (abstract.Sinker, error) {
	var result abstract.Sinker

	uncasted, err := newSinker(cfg, transferID, logger, registry, cp)
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

func newSinker(cfg yt2.YtDestinationModel, transferID string, lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator) (*sinker, error) {
	ytClient, err := ytclient.FromConnParams(cfg, lgr)
	if err != nil {
		return nil, xerrors.Errorf("error getting YT Client: %w", err)
	}

	chunkSize := int(cfg.ChunkSize())
	if len(cfg.Index()) > 0 {
		chunkSize = chunkSize / (len(cfg.Index()) + 1)
	}

	s := sinker{
		ytClient:       ytClient,
		dir:            ypath.Path(cfg.Path()),
		logger:         lgr,
		metrics:        stats.NewSinkerStats(registry),
		schemas:        map[string][]abstract.ColSchema{},
		tables:         map[string]GenericTable{},
		config:         cfg,
		lock:           maplock.NewMapMutex(),
		chunkSize:      chunkSize,
		closed:         false,
		progressInited: false,
		schemaMutex:    sync.Mutex{},
		transferID:     transferID,
		cp:             cp,
	}

	if cfg.Rotation() != nil {
		go s.runRotator()
	}

	return &s, nil
}

func (s *sinker) newGenericTable(path ypath.Path, schema []abstract.ColSchema) (GenericTable, error) {
	s.logger.Info("create generic table", log.Any("name", path), log.Any("schema", schema))
	originalSchema := schema
	if !s.config.DisableDatetimeHack() {
		schema = hackTimestamps(schema)
		s.logger.Warn("nasty hack that replace datetime -> int64", log.Any("name", path), log.Any("schema", schema))
	}
	if s.config.Ordered() {
		orderedTable, err := NewOrderedTable(s.ytClient, path, schema, s.config, s.metrics, s.logger)
		if err != nil {
			return nil, xerrors.Errorf("cannot create ordered table: %w", err)
		}
		return orderedTable, nil
	}
	if s.config.VersionColumn() != "" {
		if generic.IsGenericUnparsedSchema(abstract.NewTableSchema(schema)) &&
			strings.HasSuffix(path.String(), "_unparsed") {
			s.logger.Info("Table with unparsed schema and _unparsed postfix detected, creation of versioned table is skipped",
				log.Any("table", path), log.Any("version_column", s.config.VersionColumn()),
				log.Any("schema", schema))
		} else if _, ok := abstract.MakeFastTableSchema(schema)[abstract.ColumnName(s.config.VersionColumn())]; !ok {
			return nil, abstract.NewFatalError(xerrors.Errorf(
				"config error: detected table '%v' without column specified as version column '%v'",
				path, s.config.VersionColumn()),
			)
		} else if s.config.Rotation() != nil {
			return nil, abstract.NewFatalError(xerrors.New("rotation is not supported with versioned tables"))
		} else {
			versionedTable, err := NewVersionedTable(s.ytClient, path, schema, s.config, s.metrics, s.logger)
			if err != nil {
				return nil, xerrors.Errorf("cannot create versioned table: %w", err)
			}
			return versionedTable, nil
		}
	}

	sortedTable, err := NewSortedTable(s.ytClient, path, schema, s.config, s.metrics, s.logger)
	if err != nil {
		return nil, xerrors.Errorf("cannot create sorted table: %w", err)
	}
	if !s.config.DisableDatetimeHack() {
		// this hack force agly code, if hack is enabled we rebuild EVERY change item schema, which is very costly
		sortedTable.tableSchema = abstract.NewTableSchema(originalSchema)
	}
	return sortedTable, nil
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
