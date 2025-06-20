//go:build !disable_yt_provider

package sink

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cast"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/core/xerrors/multierr"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	yt2 "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/exp/slices"
)

type VersionedTable struct {
	ytClient       yt.Client
	path           ypath.Path
	logger         log.Logger
	metrics        *stats.SinkerStats
	schema         []abstract.ColSchema
	archiveSpawned bool
	config         yt2.YtDestinationModel
	keys           map[string]bool
	props          map[string]bool
	orderedKeys    []string
	versionCol     abstract.ColSchema
}

func (t *VersionedTable) Init() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var err error

	sc := NewSchema(t.schema, t.config, t.path)
	vInserted := false
	skippedSchema := make([]abstract.ColSchema, 0)
	for _, c := range t.schema {
		if c.ColumnName == t.config.VersionColumn() {
			t.versionCol = c
		}
		if !c.PrimaryKey && !vInserted {
			skippedSchema = append(skippedSchema, abstract.MakeTypedColSchema("__stored_version", "any", true))
			vInserted = true
		}
		skippedSchema = append(skippedSchema, c)
	}
	for _, c := range t.schema {
		t.props[c.ColumnName] = true
		t.keys[c.ColumnName] = c.PrimaryKey
		if c.PrimaryKey {
			t.orderedKeys = append(t.orderedKeys, c.ColumnName)
		}
	}

	ddlCommand := sc.IndexTables()
	ddlCommand[t.path], err = sc.Table()
	if err != nil {
		return xerrors.Errorf("Cannot prepare schema for table %s: %w", sc.path.String(), err)
	}

	if len(t.orderedKeys) != 0 {
		skippedSc := NewSchema(skippedSchema, t.config, t.path+"_skipped")
		ddlCommand[t.path+"_skipped"], err = skippedSc.Table()
		if err != nil {
			return xerrors.Errorf("Cannot prepare schema for table %s: %w", skippedSc.path.String(), err)
		}
	}

	if err := migrate.EnsureTables(ctx, t.ytClient, ddlCommand, onConflictTryAlterWithoutNarrowing(ctx, t.ytClient)); err != nil {
		t.logger.Error("Init table error", log.Error(err))
		//nolint:descriptiveerrors
		return err
	}
	return nil
}

func (t *VersionedTable) hasOnlyPKey() bool {
	return len(t.schema) == len(t.orderedKeys)
}

func (t *VersionedTable) Write(input []abstract.ChangeItem) error {
	if t == nil {
		return nil
	}
	var commitTime uint64
	typeMap := map[string]abstract.ColSchema{}
	for _, col := range t.schema {
		typeMap[col.ColumnName] = col
	}
	upd := false
	lookupKeys := make([]interface{}, 0)
	insertRows := make([]map[string]interface{}, 0)

	for _, item := range input {
		schemaCompatible, err := t.ensureSchema(item.TableSchema.Columns())
		if err != nil {
			return xerrors.Errorf("Table %s: %w", t.path.String(), err)
		}
		if !schemaCompatible {
			t.logger.Warn("Not same schema", log.Any("expected", t.schema), log.Any("actual", item.TableSchema))
			return xerrors.New("automatic schema migration currently not supported")
		}

		if item.Kind == abstract.UpdateKind {
			upd = true
		}
		if item.CommitTime > commitTime {
			commitTime = item.CommitTime
		}

		keys := map[string]interface{}{}
		row := map[string]interface{}{}
		switch item.Kind {
		case "update", "insert":
			for idx, col := range item.ColumnNames {
				if len(item.ColumnValues) <= idx || !t.props[col] {
					continue
				}
				if t.keys[col] {
					keys[col], err = RestoreWithLengthLimitCheck(typeMap[col], item.ColumnValues[idx], t.config.DiscardBigValues(), YtDynMaxStringLength)
					if err != nil {
						return xerrors.Errorf("unable to restore value for key column '%s': %w", col, err)
					}
				}
				row[col], err = RestoreWithLengthLimitCheck(typeMap[col], item.ColumnValues[idx], t.config.DiscardBigValues(), YtDynMaxStringLength)
				if err != nil {
					return xerrors.Errorf("unable to restore value for column '%s': %w", col, err)
				}
			}
			if t.hasOnlyPKey() {
				row[DummyMainTable] = nil
			}
			lookupKeys = append(lookupKeys, keys)
			insertRows = append(insertRows, row)
		case "delete":
			t.logger.Warn("Versioned table do not support deletes")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(t.config.WriteTimeoutSec())*time.Second)
	defer cancel()
	txOpts := &yt.StartTabletTxOptions{Atomicity: &yt.AtomicityNone}
	if t.config.Atomicity() == yt.AtomicityFull {
		txOpts.Atomicity = nil // "full" is the default atomicity, hence nil
	}
	tx, err := t.ytClient.BeginTabletTx(ctx, txOpts)
	if err != nil {
		t.logger.Warn("Unable to BeginTabletTx", log.Error(err))
		return xerrors.Errorf("unable to begin tx: %w", err)
	}
	t.logger.Infof("Started tx %s", tx.ID().String())
	rb := util.Rollbacks{}
	rb.Add(func() {
		if err := tx.Abort(); err != nil {
			t.logger.Warn("Unable to abort tx", log.Error(err))
		} else {
			t.logger.Debugf("TX %s aborted", tx.ID())
		}
	})
	defer rb.Do()

	var skipped []interface{}
	var newestRows []interface{}

	if len(t.orderedKeys) > 0 {
		slices.SortFunc(insertRows, func(left, right map[string]interface{}) int {
			if t.less(left[t.config.VersionColumn()], right[t.config.VersionColumn()]) {
				return -1
			}
			return 1
		})
		versions, err := t.getExistingRowVersions(ctx, tx, lookupKeys)
		if err != nil {
			return xerrors.Errorf("error getting existing row versions: %w", err)
		}
		t.logger.Debugf("Checked existing rows, got %d", len(versions))
		if len(versions) > 0 {
			skipped, newestRows = t.splitOldNewRows(insertRows, versions)
		} else {
			newestRows = yslices.Map(insertRows, func(t map[string]interface{}) interface{} { return t })
		}
	} else {
		newestRows = yslices.Map(insertRows, func(t map[string]interface{}) interface{} { return t })
	}

	t.logger.Infof("Skipped %v from %v rows (%v will be inserted)", len(skipped), len(insertRows), len(newestRows))

	if len(newestRows) > 0 {
		if err := tx.InsertRows(ctx, t.path, newestRows, &yt.InsertRowsOptions{Update: &upd}); err != nil {
			t.logger.Warn("Unable to InsertRows", log.Error(err))
			return xerrors.Errorf("insert error: %w", err)
		}
		if err := t.updateIndexes(ctx, tx, newestRows); err != nil {
			return xerrors.Errorf("error updating index table: %w", err)
		}
	}

	if len(skipped) > 0 {
		t.logger.Infof("Inserting %d skipped rows to aux table", len(skipped))
		if err := tx.InsertRows(ctx, t.path+"_skipped", skipped, nil); err != nil {
			t.logger.Warn("Unable to insert skipped rows", log.Error(err))
			return xerrors.Errorf("error writing skipped (old version) rows: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.logger.Warn("Commit Error", log.Error(err))
		return xerrors.Errorf("commit error: %w", err)
	}
	rb.Cancel()

	return nil
}

func (t *VersionedTable) updateIndexes(ctx context.Context, tx yt.TabletTx, rows []interface{}) error {
	errs := make([]error, len(t.config.Index()))
	var wg sync.WaitGroup
	for n, idx := range t.config.Index() {
		if _, ok := t.props[idx]; !ok {
			continue
		}

		wg.Add(1)
		go func(k string, n int) {
			defer wg.Done()
			idxRows := make([]interface{}, 0)
			for _, row := range rows {
				r, ok := row.(map[string]interface{})
				if !ok || r[k] == nil {
					continue
				}
				idxRow := map[string]interface{}{}
				idxRow[k] = r[k]
				idxRow[DummyIndexTable] = nil
				for _, col := range t.schema {
					if col.PrimaryKey {
						idxRow[col.ColumnName] = r[col.ColumnName]
					}
				}
				idxRows = append(idxRows, idxRow)
			}

			idxPath := ypath.Path(MakeIndexTableName(string(t.path), k))
			t.metrics.Table(string(idxPath), "rows", len(idxRows))
			t.logger.Infof("prepare idx %v %v rows", idxPath, len(idxRows))
			if err := tx.InsertRows(ctx, idxPath, idxRows, nil); err != nil {
				t.logger.Warn("Unable to InsertRows into IDX table", log.Error(err))
				errs[n] = xerrors.Errorf("index %s insert error: %w", idxPath.String(), err)
			}
		}(idx, n)
	}
	wg.Wait()
	return multierr.Combine(errs...)
}

func (t *VersionedTable) getExistingRowVersions(ctx context.Context, tx yt.TabletTx, lookupKeys []interface{}) (map[string]string, error) {
	var errChs []chan error
	var mu sync.Mutex
	versions := map[string]string{}

	for i := 0; i < len(lookupKeys); i += 500 {
		upper := i + 500
		if upper > len(lookupKeys) {
			upper = len(lookupKeys)
		}
		errCh := make(chan error, 1)
		t.logger.Debugf("Lookup keys %d:%d", i, upper)
		go func(keys []interface{}, errCh chan error) {
			var colFilter []string
			for _, key := range t.orderedKeys {
				if key != t.config.VersionColumn() {
					colFilter = append(colFilter, key)
				}
			}
			colFilter = append(colFilter, t.config.VersionColumn())
			exist, err := tx.LookupRows(ctx, t.path, keys, &yt.LookupRowsOptions{
				KeepMissingRows: false,
				Columns:         colFilter,
			})
			if err != nil {
				errCh <- xerrors.Errorf("error looking up ordered keys: %w", err)
				return
			}
			for exist.Next() {
				var row map[string]interface{}
				if err := exist.Scan(&row); err != nil {
					errCh <- xerrors.Errorf("error scaning ordered keys: %w", err)
					return
				}
				keyV := make([]string, len(t.orderedKeys))
				for i, key := range t.orderedKeys {
					keyV[i] = fmt.Sprintf("%v", row[key])
				}
				mu.Lock()
				versions[strings.Join(keyV, ",")] = fmt.Sprintf("%v", row[t.config.VersionColumn()])
				mu.Unlock()
			}
			errCh <- nil
		}(lookupKeys[i:upper], errCh)
		errChs = append(errChs, errCh)
	}
	var errs util.Errors
	for _, ch := range errChs {
		errs = util.AppendErr(errs, <-ch)
	}
	if len(errs) != 0 {
		return nil, errs
	}
	return versions, nil
}

func (t *VersionedTable) splitOldNewRows(insertRows []map[string]interface{}, versions map[string]string) (oldRows, newRows []interface{}) {
	for _, row := range insertRows {
		targetVersion := fmt.Sprintf("%v", row[t.config.VersionColumn()])
		keyV := make([]string, len(t.orderedKeys))
		for i, key := range t.orderedKeys {
			keyV[i] = fmt.Sprintf("%v", row[key])
		}
		existVersion, ok := versions[strings.Join(keyV, ",")]
		if !ok {
			newRows = append(newRows, row)
			continue
		}
		if !t.less(existVersion, targetVersion) {
			row["__stored_version"] = existVersion
			oldRows = append(oldRows, row)
		} else {
			newRows = append(newRows, row)
		}
	}
	return oldRows, newRows
}

func (t *VersionedTable) ensureSchema(schemas []abstract.ColSchema) (schemaCompatible bool, err error) {
	if t.config.IsSchemaMigrationDisabled() || !t.config.CanAlter() {
		return true, nil
	}
	if !t.config.DisableDatetimeHack() {
		schemas = hackTimestamps(schemas)
	}

	if schemasAreEqual(t.schema, schemas) {
		return true, nil
	}

	t.logger.Warn("Schema alter detected", log.Any("current", t.schema), log.Any("target", schemas))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	table, err := t.buildTargetTable(schemas)
	if err != nil {
		return false, err
	}
	alterCommand := map[ypath.Path]migrate.Table{t.path: table}
	skippedSchema := make([]abstract.ColSchema, 0)
	vInserted := false
	for _, c := range t.schema {
		if !c.PrimaryKey && !vInserted {
			skippedSchema = append(skippedSchema, abstract.MakeTypedColSchema("__stored_version", "any", true))
			vInserted = true
		}
		skippedSchema = append(skippedSchema, c)
	}
	alterCommand[t.path+"_skipped"], _ = t.buildTargetTable(skippedSchema)
	t.logger.Warn("Init alter command", log.Any("alter", alterCommand))
	if err := migrate.EnsureTables(ctx, t.ytClient, alterCommand, onConflictTryAlterWithoutNarrowing(ctx, t.ytClient)); err != nil {
		t.logger.Error("Unable to migrate schema", log.Error(err))
		return false, nil
	}

	t.schema = schemas
	t.keys = map[string]bool{}
	for _, col := range t.schema {
		if col.PrimaryKey {
			t.keys[col.ColumnName] = true
		}
	}
	if len(t.keys) == 0 {
		t.logger.Warnf("Table %s: %v", t.path.String(), NoKeyColumnsFound)
		return false, nil
	}

	return true, nil
}

func (t *VersionedTable) buildTargetTable(schemas []abstract.ColSchema) (migrate.Table, error) {
	s := true
	haveKeyColumns := false
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

		if col.PrimaryKey {
			target.Columns[i].SortOrder = schema.SortAscending
			haveKeyColumns = true
		}
	}
	if !haveKeyColumns {
		return migrate.Table{}, abstract.NewFatalError(NoKeyColumnsFound)
	}
	return migrate.Table{
		Attributes: nil,
		Schema:     target,
	}, nil
}

// less will check whether left *less* than right
// it will give asc order for standard slices sort
func (t *VersionedTable) less(left, right interface{}) bool {
	switch schema.Type(t.versionCol.DataType) {
	case schema.TypeFloat64, schema.TypeFloat32:
		return cast.ToFloat64(left) < cast.ToFloat64(right)
	case schema.TypeInt64, schema.TypeInt32, schema.TypeInt16, schema.TypeInt8:
		return cast.ToInt64(left) < cast.ToInt64(right)
	case schema.TypeUint64, schema.TypeUint32, schema.TypeUint16, schema.TypeUint8:
		return cast.ToUint64(left) < cast.ToUint64(right)
	default:
		return fmt.Sprintf("%v", left) < fmt.Sprintf("%v", right)
	}
}

func NewVersionedTable(ytClient yt.Client, path ypath.Path, schema []abstract.ColSchema, cfg yt2.YtDestinationModel, metrics *stats.SinkerStats, logger log.Logger) (GenericTable, error) {
	var dummyVersionCol abstract.ColSchema
	t := VersionedTable{
		ytClient:       ytClient,
		path:           path,
		logger:         logger,
		metrics:        metrics,
		schema:         schema,
		archiveSpawned: false,
		config:         cfg,
		keys:           map[string]bool{},
		props:          map[string]bool{},
		orderedKeys:    make([]string, 0),
		versionCol:     dummyVersionCol,
	}

	if err := t.Init(); err != nil {
		return nil, err
	}

	return &t, nil
}
