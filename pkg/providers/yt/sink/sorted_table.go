//go:build !disable_yt_provider

package sink

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	yt2 "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/sync/semaphore"
)

type SortedTable struct {
	ytClient       yt.Client
	path           ypath.Path
	logger         log.Logger
	metrics        *stats.SinkerStats
	columns        tableColumns // useful wrapper over []abstract.ColSchema
	archivePath    ypath.Path
	archiveSpawned bool
	config         yt2.YtDestinationModel
	sem            *semaphore.Weighted
	tableSchema    *changeitem.TableSchema
}

func (t *SortedTable) Init() error {
	var err error
	tableSchema := NewSchema(t.columns.columns, t.config, t.path)
	ddlCommand := tableSchema.IndexTables()
	ddlCommand[t.path], err = tableSchema.Table()
	if err != nil {
		return xerrors.Errorf("Cannot prepare schema for table %s: %w", t.path.String(), err)
	}

	logger.Log.Info("prepared ddlCommand for migrate", log.Any("path", t.path), log.Any("attrs", ddlCommand[t.path].Attributes), log.Any("schema", ddlCommand[t.path].Schema))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := migrate.EnsureTables(ctx, t.ytClient, ddlCommand, onConflictTryAlterWithoutNarrowing(ctx, t.ytClient)); err != nil {
		t.logger.Error("Init table error", log.Error(err))
		return err
	}

	return nil
}

func (t *SortedTable) Insert(insertRows []interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(t.config.WriteTimeoutSec())*time.Second)
	defer cancel()

	tx, rollbacks, err := beginTabletTransaction(ctx, t.ytClient, t.config.Atomicity() == yt.AtomicityFull, t.logger)
	if err != nil {
		return xerrors.Errorf("Unable to beginTabletTransaction: %w", err)
	}
	defer rollbacks.Do()

	if err := tx.InsertRows(ctx, t.path, insertRows, nil); err != nil {
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

func getCommitTime(input []abstract.ChangeItem) uint64 {
	var commitTime uint64
	for _, item := range input {
		if item.CommitTime > commitTime {
			commitTime = item.CommitTime
		}
	}
	return commitTime
}

//nolint:descriptiveerrors
func (t *SortedTable) dispatchItem(dataBatch *ytDataBatch, kind abstract.Kind, item changeItemView) error {
	switch kind {
	case abstract.UpdateKind:
		return dataBatch.addUpdate(item)
	case abstract.InsertKind:
		return dataBatch.addInsert(item)
	case abstract.DeleteKind:
		return dataBatch.addDelete(item)
	}
	return nil
}

func (t *SortedTable) prepareDataRows(input []abstract.ChangeItem, commitTime uint64) (ytDataBatch, error) {
	var upd bool
	var dataBatch ytDataBatch
	dataBatch.insertOptions.Update = &upd
	dataBatch.deleteRows = t.makeDataRowDeleter(commitTime)
	if changeitem.InsertsOnly(input) {
		dataBatch.toInsert = make([]any, 0, len(input))
	}

	for _, item := range input {
		if item.Kind == abstract.UpdateKind {
			upd = true
		}

		itemView := newDataItemView(&item, &t.columns, t.config.DiscardBigValues())

		if err := t.dispatchItem(&dataBatch, item.Kind, &itemView); err != nil {
			return ytDataBatch{}, xerrors.Errorf("Cannot dispatch input item of kind %s: %w", item.Kind, err)
		}
	}

	return dataBatch, nil
}

type tablePath = ypath.Path

func (t *SortedTable) prepareIndexRows(ctx context.Context, input []abstract.ChangeItem) (map[tablePath]*ytDataBatch, error) {
	index := map[tablePath]*ytDataBatch{}
	if len(t.config.Index()) == 0 {
		return index, nil
	}

	oldRows, err := t.getOldRows(ctx, input)
	if err != nil {
		return nil, xerrors.Errorf("Cannot retrieve old rows: %w", err)
	}

	for i := range input {
		item := &input[i]
		for _, indexColumnName := range t.config.Index() {
			itemView, err := newIndexItemView(item, &t.columns, indexColumnName, oldRows[i], t.config.DiscardBigValues())
			if err != nil {
				if xerrors.Is(err, noIndexColumn) {
					// TODO: this is ugly. It happens for each row of a table which doesn't have a column
					// named indexColumn. We should qualify index column names in config with their respective
					// table and schema names and do not check every incoming row for fitting the index column name.
					continue
				} else {
					return nil, xerrors.Errorf("Cannot create index view over a change item: %w", err)
				}
			}

			indexTablePath := ypath.Path(MakeIndexTableName(string(t.path), indexColumnName))
			batch, ok := index[indexTablePath]
			if !ok {
				batch = new(ytDataBatch)
				batch.deleteRows = indexRowDeleter
				index[indexTablePath] = batch
			}

			if err := t.dispatchItem(batch, item.Kind, &itemView); err != nil {
				return nil, xerrors.Errorf("Cannot dispatch input item of kind %s: %w", item.Kind, err)
			}
		}
	}
	return index, nil
}

func (t *SortedTable) getOldRows(ctx context.Context, input []abstract.ChangeItem) ([]ytRow, error) {
	result := make([]ytRow, len(input))
	var keys []interface{}
	var backrefs []int

	for i := range input {
		item := &input[i]
		if item.Kind != abstract.UpdateKind && item.Kind != abstract.DeleteKind {
			continue
		}

		dataView := newDataItemView(item, &t.columns, t.config.DiscardBigValues())
		key, err := dataView.makeOldKeys()
		if err != nil {
			return nil, xerrors.Errorf("Cannot create change item key: %w", err)
		}
		keys = append(keys, key)
		backrefs = append(backrefs, i)
	}

	if len(keys) == 0 {
		return result, nil
	}

	reader, err := t.ytClient.LookupRows(ctx, t.path, keys, &yt.LookupRowsOptions{KeepMissingRows: true})
	if err != nil {
		return nil, xerrors.Errorf("Cannot lookup old values for updated and deleted rows: %w", err)
	}
	defer reader.Close()

	i := 0
	for reader.Next() {
		var oldRow ytRow
		if err := reader.Scan(&oldRow); err != nil {
			return nil, xerrors.Errorf("Cannot parse row from YT: %w", err)
		}
		if i >= len(backrefs) {
			return nil, xerrors.Errorf("Extra data returned from YT")
		}
		itemIndex := backrefs[i]
		result[itemIndex] = oldRow
		i++
	}
	if reader.Err() != nil {
		return nil, xerrors.Errorf("Cannot read row from YT: %w", reader.Err())
	}

	return result, nil
}

func (t *SortedTable) makeDataRowDeleter(commitTime uint64) deleteRowsFn {
	return func(ctx context.Context, tx yt.TabletTx, tablePath ypath.Path, keys []interface{}) error {
		return t.deleteAndArchiveRows(ctx, tx, tablePath, keys, commitTime)
	}
}

func indexRowDeleter(ctx context.Context, tx yt.TabletTx, tablePath ypath.Path, keys []interface{}) error {
	return tx.DeleteRows(ctx, tablePath, keys, nil)
}

// Write accept input which will be collapsed as very first step
func (t *SortedTable) Write(input []abstract.ChangeItem) error {
	input = abstract.Collapse(input)
	if len(t.config.Index()) > 0 {
		// TODO: this was added for TM-702, but it doesn't look helpful for that issue.
		// We should probably get rid of the semaphore entirely, or at least remove the condition above.
		_ = t.sem.Acquire(context.TODO(), 1)
		defer t.sem.Release(1)
	}
	if t == nil {
		return nil
	}

	for _, item := range input {
		if t.tableSchema.Equal(item.TableSchema) {
			continue
		}
		schemaCompatible, err := t.ensureSchema(item.TableSchema.Columns())
		if err != nil {
			return xerrors.Errorf("Table %s: %w", t.path.String(), err)
		}
		if !schemaCompatible {
			return xerrors.Errorf("Incompatible schema change detected: expected %v; actual %v", t.columns.columns, item.TableSchema.Columns())
		}
	}

	commitTime := getCommitTime(input)
	dataBatch, err := t.prepareDataRows(input, commitTime)
	if err != nil {
		return xerrors.Errorf("Cannot prepare data input for YT: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(t.config.WriteTimeoutSec())*time.Second)
	defer cancel()

	index, err := t.prepareIndexRows(ctx, input)
	if err != nil {
		return xerrors.Errorf("Cannot prepare index input for YT: %w", err)
	}

	tx, rollbacks, err := beginTabletTransaction(ctx, t.ytClient, t.config.Atomicity() == yt.AtomicityFull, t.logger)
	if err != nil {
		return xerrors.Errorf("Unable to beginTabletTransaction: %w", err)
	}
	defer rollbacks.Do()

	if err := dataBatch.process(ctx, tx, t.path); err != nil {
		return xerrors.Errorf("Cannot process data batch for table %s: %w", t.path, err)
	}
	for indexTablePath, indexBatch := range index {
		t.metrics.Table(string(indexTablePath), "rows", len(indexBatch.toInsert))
		t.logger.Infof("prepare %v %v rows", indexTablePath, len(indexBatch.toInsert))
		if err := indexBatch.process(ctx, tx, indexTablePath); err != nil {
			return xerrors.Errorf("Cannot process data batch for index table %s: %w", indexTablePath, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return xerrors.Errorf("Cannot commit transaction: %w", err)
	}

	rollbacks.Cancel()
	return nil
}

func (t *SortedTable) ensureSchema(schemas []abstract.ColSchema) (schemaCompatible bool, err error) {
	if t.config.IsSchemaMigrationDisabled() || !t.config.CanAlter() {
		return true, nil
	}
	if !t.config.DisableDatetimeHack() {
		schemas = hackTimestamps(schemas)
	}

	if schemasAreEqual(t.columns.columns, schemas) {
		return true, nil
	}

	t.logger.Warn("Schema alter detected", log.Any("current", t.columns.columns), log.Any("target", schemas))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	newTable, err := t.buildTargetTable(schemas)
	if err != nil {
		return false, err
	}
	alterCommand := map[ypath.Path]migrate.Table{t.path: newTable}
	t.logger.Warn("Init alter command", log.Any("alter", alterCommand))
	if err := migrate.EnsureTables(ctx, t.ytClient, alterCommand, onConflictTryAlterWithoutNarrowing(ctx, t.ytClient)); err != nil {
		t.logger.Error("Unable to migrate schema", log.Error(err))
		return false, nil
	}

	t.columns = newTableColumns(schemas)
	return true, nil
}

func (t *SortedTable) buildTargetTable(schemas []abstract.ColSchema) (migrate.Table, error) {
	s := true
	hasKeyColumns := false
	target := schema.Schema{
		UniqueKeys: true,
		Strict:     &s,
		Columns:    make([]schema.Column, len(schemas)),
	}
	for i, col := range schemas {
		target.Columns[i] = schema.Column{
			Name:       col.ColumnName,
			Type:       schema.Type(col.DataType),
			Expression: col.Expression,
		}

		if col.IsKey() {
			target.Columns[i].SortOrder = schema.SortAscending
			hasKeyColumns = true
		}
	}
	if !hasKeyColumns {
		return migrate.Table{}, abstract.NewFatalError(NoKeyColumnsFound)
	}
	return migrate.Table{Schema: target}, nil
}

//nolint:descriptiveerrors
func (t *SortedTable) deleteAndArchiveRows(ctx context.Context, tx yt.TabletTx, tablePath ypath.Path, elems []interface{}, commitTS uint64) error {
	if len(elems) == 0 {
		return nil
	}

	if err := t.spawnArchive(ctx); err != nil {
		return err
	}

	if t.archiveSpawned {
		reader, err := tx.LookupRows(ctx, tablePath, elems, nil)
		if err != nil {
			return err
		}

		oldRows := make([]interface{}, 0)
		for reader.Next() {
			oldRow := map[string]interface{}{}
			if err := reader.Scan(&oldRow); err != nil {
				return err
			}

			oldRow["commit_time"] = commitTS
			oldRows = append(oldRows, oldRow)
		}
		t.metrics.Table(string(t.archivePath), "rows", len(oldRows))
		if err := tx.InsertRows(ctx, t.archivePath, oldRows, nil); err != nil {
			return err
		}
	}

	if err := tx.DeleteRows(ctx, tablePath, elems, nil); err != nil {
		return err
	}

	return nil
}

func (t *SortedTable) spawnArchive(ctx context.Context) error {
	if t.archiveSpawned || !t.config.NeedArchive() {
		return nil
	}

	archiveSchema := []abstract.ColSchema{{
		ColumnName: "commit_time",
		DataType:   "int64",
		PrimaryKey: true,
	}}

	baseTableInfo, err := yt2.GetNodeInfo(ctx, t.ytClient, t.path)
	if err != nil {
		t.logger.Errorf("cannot get base table %v schema: %v", t.path, err)
		archiveSchema = append(archiveSchema, t.columns.columns...)
	} else {
		baseSchema := yt2.YTColumnToColSchema(baseTableInfo.Attrs.Schema.Columns)
		archiveSchema = append(archiveSchema, baseSchema.Columns()...)
	}

	if err := backoff.Retry(func() error {
		var ytDestination yt2.YtDestination
		ytDestination.Cluster = t.config.Cluster()
		ytDestination.CellBundle = t.config.CellBundle()
		ytDestination.OptimizeFor = t.config.OptimizeFor()
		ytDestination.CanAlter = t.config.CanAlter()
		ytDestination.DisableDatetimeHack = t.config.DisableDatetimeHack()
		ytDestination.PrimaryMedium = t.config.PrimaryMedium()
		ytDestination.LoseDataOnError = t.config.LoseDataOnError()
		_, err := NewSortedTable(
			t.ytClient,
			t.archivePath,
			archiveSchema,
			yt2.NewYtDestinationV1(ytDestination),
			t.metrics,
			log.With(t.logger, log.Any("table_path", t.path)),
		)
		if err != nil {
			t.logger.Warn("unable to init archive tablet, try to delete it", log.Error(err))
			_ = t.ytClient.RemoveNode(context.TODO(), t.archivePath, &yt.RemoveNodeOptions{
				Force: true,
			})
		}
		return err
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)); err != nil {
		return err
	}

	t.archiveSpawned = true
	return nil
}

func NewSortedTable(
	ytClient yt.Client,
	path ypath.Path,
	schema []abstract.ColSchema,
	cfg yt2.YtDestinationModel,
	metrics *stats.SinkerStats,
	logger log.Logger,
) (*SortedTable, error) {
	t := SortedTable{
		ytClient:       ytClient,
		path:           path,
		logger:         logger,
		metrics:        metrics,
		columns:        newTableColumns(schema),
		archivePath:    ypath.Path(fmt.Sprintf("%v_archive", string(path))),
		archiveSpawned: false,
		config:         cfg,
		sem:            semaphore.NewWeighted(10),
		tableSchema:    changeitem.NewTableSchema(schema),
	}

	if err := t.Init(); err != nil {
		return nil, err
	}

	return &t, nil
}
