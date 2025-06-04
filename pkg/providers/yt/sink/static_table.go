package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	yt2 "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/mapreduce"
	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type StaticTable struct {
	ytClient      yt.Client
	path          ypath.Path
	logger        log.Logger
	txMutex       sync.Mutex
	tablesTxs     map[abstract.TableID]yt.Tx
	wrMutex       sync.Mutex
	tablesWriters map[abstract.TableID]*tableWriter
	spec          map[string]interface{}
	config        yt2.YtDestinationModel
	metrics       *stats.SinkerStats
}

type tableWriter struct {
	target    ypath.Path
	tmp       ypath.Path
	wr        yt.TableWriter
	runningTx yt.Tx
}

func (t *StaticTable) Close() error {
	if err := t.rollbackAll(); err != nil {
		return xerrors.Errorf("failed to rollback: %w", err)
	}
	return nil
}

func (t *StaticTable) rollbackAll() error {
	t.logger.Info("rollback all transactions")

	defer func() {
		t.tablesWriters = map[abstract.TableID]*tableWriter{}
		t.tablesTxs = map[abstract.TableID]yt.Tx{}
	}()

	for tName, tx := range t.tablesTxs {
		if tx != nil {
			t.logger.Info("rollback transaction", log.Any("transaction", tx.ID()), log.Any("table", tName))
			if err := tx.Abort(); err != nil {
				t.logger.Error("cannot abort transaction", log.Any("table", tName), log.Any("transaction", tx.ID()), log.Error(err))
				return err
			}
		}
	}
	return nil
}

func (t *StaticTable) begin(tableID abstract.TableID) error {
	t.txMutex.Lock()
	defer t.txMutex.Unlock()

	if _, ok := t.tablesTxs[tableID]; ok {
		t.logger.Errorf("transaction for table %v already began", tableID.Fqtn())
		return xerrors.Errorf("transaction for table %v already began", tableID.Fqtn())
	}

	ctx := context.Background()
	tx, err := t.ytClient.BeginTx(ctx, nil)
	if err != nil {
		t.logger.Error("cannot begin internal transaction for table", log.Any("table", tableID.Fqtn()), log.Error(err))
		return err
	}
	t.tablesTxs[tableID] = tx

	t.metrics.Inflight.Inc()
	return nil
}

func (t *StaticTable) getTx(tableID abstract.TableID) (tx yt.Tx, ok bool) {
	t.txMutex.Lock()
	defer t.txMutex.Unlock()

	tx, ok = t.tablesTxs[tableID]
	return tx, ok
}

func (t *StaticTable) commit(tableID abstract.TableID) error {
	defer func() {
		t.txMutex.Lock()
		defer t.txMutex.Unlock()
		delete(t.tablesWriters, tableID)
		delete(t.tablesTxs, tableID)
	}()

	tx, ok := t.getTx(tableID)
	if !ok {
		t.logger.Error("cannot commit: transaction for table was not started", log.Any("table", tableID.Fqtn()))
		return xerrors.Errorf("cannot commit: transaction for table %v was not started", tableID.Fqtn())
	}

	twr, ok := t.getWriter(tableID)
	if !ok {
		t.logger.Infof("there were no writes for table %v, commit empty transaction", tableID.Fqtn())
		if err := tx.Commit(); err != nil {
			t.logger.Error("cannot commit empty transaction", log.Any("table", tableID.Fqtn()), log.Error(err))
			return xerrors.Errorf("cannot commit empty table %v: %w", tableID.Fqtn(), err)
		}
		return nil
	}

	if twr.runningTx != nil {
		t.logger.Info("try commit", log.Any("table", tableID.Fqtn()), log.Any("transaction", twr.runningTx.ID()), log.Any("path", twr.target))
		ctx := context.Background()
		if err := twr.wr.Commit(); err != nil {
			t.logger.Error("cannot commit table writer, aborting transaction", log.Any("table", tableID.Fqtn()), log.Any("transaction", twr.runningTx.ID()))
			_ = twr.runningTx.Abort()
			//nolint:descriptiveerrors
			return err
		}

		if err := t.mergeIfNeeded(ctx, twr); err != nil {
			_ = twr.runningTx.Abort()
			return xerrors.Errorf("unable to merge: %w", err)
		}

		moveOptions := yt2.ResolveMoveOptions(twr.runningTx, twr.tmp, false)
		if _, err := twr.runningTx.MoveNode(ctx, twr.tmp, twr.target, moveOptions); err != nil {
			t.logger.Error("cannot move tmp table, aborting transaction", log.Any("table", tableID.Fqtn()), log.Any("transaction", twr.runningTx.ID()), log.Any("path", twr.tmp))
			_ = twr.runningTx.Abort()
			//nolint:descriptiveerrors
			return err
		}
		if err := twr.runningTx.Commit(); err != nil {
			t.logger.Error("cannot commit transaction, aborting...", log.Any("table", tableID.Fqtn()), log.Any("transaction", twr.runningTx.ID()))
			//nolint:descriptiveerrors
			return err
		}
	}
	return nil
}

func (t *StaticTable) mergeIfNeeded(ctx context.Context, tableWriter *tableWriter) error {
	if t.config == nil || t.config.CleanupMode() != model.DisabledCleanup {
		return nil
	}

	targetExists, err := tableWriter.runningTx.NodeExists(ctx, tableWriter.target, nil)
	if err != nil {
		return xerrors.Errorf("unable to check if target table '%v' exists", err)
	} else if !targetExists {
		return nil
	}

	mrClient := mapreduce.New(t.ytClient).WithTx(tableWriter.runningTx)
	mergeSpec := spec.Merge()
	mergeSpec.MergeMode = "ordered"
	mergeSpec.InputTablePaths = []ypath.YPath{tableWriter.target, tableWriter.tmp}
	mergeSpec.OutputTablePath = tableWriter.tmp
	mergeSpec.Pool = "transfer_manager"
	mergeOperation, err := mrClient.Merge(mergeSpec)
	if err != nil {
		return xerrors.Errorf("unable to start merge: %w", err)
	}

	t.logger.Infof("started merging target '%v' and tmp '%v'", tableWriter.target, tableWriter.tmp)
	err = mergeOperation.Wait()
	if err == nil {
		t.logger.Infof("successfully merged target '%v' and tmp '%v'", tableWriter.target, tableWriter.tmp)
	}
	return err
}

func staticYTSchema(item abstract.ChangeItem) []schema.Column {
	result := yt2.ToYtSchema(item.TableSchema.Columns(), false)

	for i := range result {
		// Static table should not be ordered
		result[i].SortOrder = ""
		result[i].Expression = ""
	}
	return result
}

func getNameFromTableID(tID abstract.TableID) string {
	if tID.Namespace == "public" || len(tID.Namespace) == 0 {
		return tID.Name
	}
	return fmt.Sprintf("%s_%s", tID.Namespace, tID.Name)
}

func columnSchemaByName(s abstract.TableColumns) map[string]abstract.ColSchema {
	result := map[string]abstract.ColSchema{}
	for _, c := range s {
		result[c.ColumnName] = c
	}
	return result
}

func (t *StaticTable) Push(items []abstract.ChangeItem) error {
	start := time.Now()
	rowsPushedByTable := map[abstract.TableID]int{}

	ctx := context.Background()
	if len(items) == 0 {
		return nil
	}

	var prevTableID abstract.TableID
	var writer *tableWriter = nil
	colSchemaByNameByTable := map[abstract.TableID]map[string]abstract.ColSchema{}
	var colSchemaByName map[string]abstract.ColSchema
	for _, item := range items {
		tableID := item.TableID()

		switch item.Kind {
		case abstract.InsertKind:
			if prevTableID != tableID {
				ok := false

				writer, ok = t.getWriter(tableID)
				if !ok {
					if err := t.addWriter(ctx, tableID, item); err != nil {
						t.metrics.Table(tableID.Fqtn(), "error", 1)
						t.logger.Error("cannot create table writer", log.Any("table", tableID), log.Error(err))
						return err
					}
					writer, _ = t.getWriter(tableID)
				}

				colSchemaByName, ok = colSchemaByNameByTable[tableID]
				if !ok {
					colSchemaByName = columnSchemaByName(item.TableSchema.Columns())
					colSchemaByNameByTable[tableID] = colSchemaByName
				}
			}
			prevTableID = tableID

			row := map[string]interface{}{}
			for i, columnName := range item.ColumnNames {
				colSchema, ok := colSchemaByName[columnName]
				if !ok {
					t.logger.Error("Found unknown column in schema.",
						log.Any("schema_before", colSchemaByName),
						log.Any("current_item_schema", columnSchemaByName(item.TableSchema.Columns())))
					return xerrors.Errorf("unknown column to get schema: %s", columnName)
				}
				var err error
				row[columnName], err = RestoreWithLengthLimitCheck(colSchema, item.ColumnValues[i], false, YtStatMaxStringLength)
				if err != nil {
					return xerrors.Errorf("failed to restore value for column '%s': %w", columnName, err)
				}
			}
			if err := writer.wr.Write(row); err != nil {
				t.metrics.Table(tableID.Fqtn(), "error", 1)
				s, _ := json.MarshalIndent(item, "", "    ")
				logger.Log.Error("cannot write changeItem", log.Any("table", writer.tmp), log.Error(err), log.String("item", string(s)))
				return abstract.NewTableUploadError(err)
			}
			rowsPushedByTable[tableID] += 1
		case abstract.InitTableLoad:
			if err := t.begin(tableID); err != nil {
				return xerrors.Errorf("failed to BEGIN transaction for table %s: %w", tableID.Fqtn(), err)
			}
		case abstract.DoneTableLoad:
			if err := t.commit(tableID); err != nil {
				return xerrors.Errorf("failed to COMMIT transaction for table %s: %w", tableID.Fqtn(), err)
			}
		default:
			continue
		}
	}

	for tableID, rowsPushed := range rowsPushedByTable {
		t.metrics.Table(tableID.Fqtn(), "rows", rowsPushed)
	}
	t.metrics.Elapsed.RecordDuration(time.Since(start))

	return nil
}

func (t *StaticTable) getWriter(tID abstract.TableID) (twr *tableWriter, ok bool) {
	t.wrMutex.Lock()
	defer t.wrMutex.Unlock()

	twr, ok = t.tablesWriters[tID]
	return twr, ok
}

func (t *StaticTable) getTableName(tID abstract.TableID, item abstract.ChangeItem) ypath.Path {
	if t.config == nil {
		return yt2.SafeChild(t.path, getNameFromTableID(tID))
	} else {
		target := yt2.SafeChild(t.path, t.config.GetTableAltName(getNameFromTableID(tID)))
		if t.config != nil && t.config.Rotation() != nil {
			target = yt2.SafeChild(t.path, t.config.Rotation().AnnotateWithTimeFromColumn(t.config.GetTableAltName(getNameFromTableID(tID)), item))
		}
		return target
	}
}

func (t *StaticTable) addWriter(ctx context.Context, tID abstract.TableID, item abstract.ChangeItem) error {
	ytSchema := staticYTSchema(item)
	if ytSchema == nil {
		return nil // or we should return error?
	}

	target := t.getTableName(tID, item)
	tmpTablePath := ypath.Path(fmt.Sprintf("%v_%v", target, getRandomPostfix()))

	tmpTableDirPath := getDirPath(tmpTablePath)
	if _, err := t.ytClient.CreateNode(ctx, tmpTableDirPath, yt.NodeMap, &yt.CreateNodeOptions{
		IgnoreExisting: true,
		Recursive:      true,
	}); err != nil {
		return xerrors.Errorf("cannot create directory node for table %s: %w", tmpTablePath, err)
	}

	t.wrMutex.Lock()
	defer t.wrMutex.Unlock()
	if _, ok := t.tablesWriters[tID]; !ok {
		tx, ok := t.getTx(tID)
		if !ok {
			t.logger.Error("cannot init table writer: transaction was not started", log.Any("table", tID))
			return abstract.NewFatalError(xerrors.Errorf("cannot create table writer for table %v: transaction was not started", tID))
		}

		createOptions := yt.CreateNodeOptions{
			Attributes: map[string]interface{}{
				"schema":      ytSchema,
				"unique_keys": false,
				"strict":      true,
			},
			TransactionOptions: &yt.TransactionOptions{},
			Recursive:          true,
			IgnoreExisting:     false,
		}
		if t.config != nil {
			createOptions.Attributes["optimize_for"] = t.config.OptimizeFor()
			createOptions.Attributes = t.config.MergeAttributes(createOptions.Attributes)
		}
		logger.Log.Info(
			"Creating YT table  with options",
			log.String("tmpPath", tmpTablePath.String()),
			log.Any("options", createOptions),
		)

		if _, err := tx.CreateNode(ctx, tmpTablePath, yt.NodeTable, &createOptions); err != nil {
			//nolint:descriptiveerrors
			return err
		}
		opts := &yt.WriteTableOptions{TableWriter: t.spec}
		w, err := tx.WriteTable(ctx, tmpTablePath, opts)
		if err != nil {
			return xerrors.Errorf("unable to create table writer: %w", err)
		}
		t.logger.Info("add new writer", log.Any("table", tID), log.Any("transaction", tx.ID()))
		t.tablesWriters[tID] = &tableWriter{
			runningTx: tx,
			target:    target,
			tmp:       tmpTablePath,
			wr:        w,
		}
	}
	return nil
}

func getDirPath(tablePath ypath.Path) ypath.Path {
	return ypath.Path(ypath.Root.String() + path.Dir(tablePath.String()))
}

func getRandomPostfix() string {
	return fmt.Sprintf("transited_at_%v", time.Now().Format("2006-01-02_15:04:05"))
}

func NewStaticTableFromConfig(ytClient yt.Client, cfg yt2.YtDestinationModel, registry metrics.Registry, lgr log.Logger, cp coordinator.Coordinator, transferID string) *StaticTable {
	return &StaticTable{
		ytClient:      ytClient,
		path:          ypath.Path(cfg.Path()),
		logger:        lgr,
		txMutex:       sync.Mutex{},
		tablesTxs:     map[abstract.TableID]yt.Tx{},
		wrMutex:       sync.Mutex{},
		tablesWriters: map[abstract.TableID]*tableWriter{},
		spec:          cfg.Spec().GetConfig(),
		config:        cfg,
		metrics:       stats.NewSinkerStats(registry),
	}
}

func NewStaticTable(ytClient yt.Client, path ypath.Path, ytSpec map[string]interface{}, registry metrics.Registry) *StaticTable {
	return &StaticTable{
		ytClient:      ytClient,
		path:          path,
		logger:        logger.Log,
		txMutex:       sync.Mutex{},
		tablesTxs:     map[abstract.TableID]yt.Tx{},
		wrMutex:       sync.Mutex{},
		tablesWriters: map[abstract.TableID]*tableWriter{},
		spec:          ytSpec,
		config:        nil,
		metrics:       stats.NewSinkerStats(registry),
	}
}
