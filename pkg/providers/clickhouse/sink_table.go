package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"runtime/debug"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/blang/semver/v4"
	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/providers/clickhouse/columntypes"
	"github.com/transferia/transferia/pkg/providers/clickhouse/errors"
	httpuploader2 "github.com/transferia/transferia/pkg/providers/clickhouse/httpuploader"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/schema"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type sinkTable struct {
	server     *SinkServer
	tableName  string
	config     model.ChSinkServerParams
	logger     log.Logger
	colTypes   columntypes.TypeMapping
	cols       *abstract.TableSchema // warn: schema can be changed inflight
	metrics    *stats.ChStats
	avgRowSize int
	cluster    *sinkCluster
	timezone   *time.Location
	version    semver.Version
}

// see: https://github.com/Altinity/clickhouse-sink-connector/issues/206#issuecomment-1529968850
var deleteableVersion = semver.MustParse("23.2.0")

func normalizeTableName(table string) string {
	res := strings.ReplaceAll(table, "-", "_")
	return strings.ReplaceAll(res, ".", "_")
}

func (t *sinkTable) Init(cols *abstract.TableSchema) error {
	sch := NewSchema(cols.Columns(), t.config.SystemColumnsFirst(), t.tableName)
	t.cols = cols
	exist, err := t.checkExist()
	if err != nil {
		return xerrors.Errorf("failed to check existing of table %s: %w", t.tableName, err)
	}
	if t.config.InferSchema() || exist {
		if t.config.MigrationOptions().AddNewColumns {
			targetCols, err := schema.DescribeTable(t.server.db, t.config.Database(), t.tableName, nil)
			if err != nil {
				return xerrors.Errorf("failed to discover existing schema of %s: %w", t.tableName, err)
			}
			err = t.ApplySchemaDiffToDB(targetCols.Columns(), cols.Columns())
			if err != nil {
				return xerrors.Errorf("failed to apply schema diff to %s: %w", t.tableName, err)
			}
		}
		if err := t.fillColNameToType(); err != nil {
			return xerrors.Errorf("failed to infer columns of existing table: %w", err)
		}
		t.logger.Infof("do not create table %v, infer schema: %v, exist: %v", t.tableName, t.config.InferSchema(), exist)
		return nil
	}

	err = t.cluster.execDDL(func(distributed bool) error {
		return t.createTable(sch.abstractCols(), distributed)
	})
	if err != nil {
		if errors.IsFatalClickhouseError(err) {
			return abstract.NewFatalError(err)
		}
		return xerrors.Errorf("failed to create table %s: %w", t.tableName, err)
	}

	if err := t.fillColNameToType(); err != nil {
		return xerrors.Errorf("failed to infer columns: %w", err)
	}

	return nil
}

func (t *sinkTable) fillColNameToType() error {
	t.colTypes = make(columntypes.TypeMapping)
	tableID := abstract.NewTableID(t.config.Database(), t.tableName)
	t.logger.Infof("Loading columns for table %s", tableID.String())
	q, err := t.server.db.Query(`select name, type from system.columns where database = ? and table = ?;`, tableID.Namespace, tableID.Name)
	if err != nil {
		return xerrors.Errorf("failed to discover schema of '%s': %w", tableID.String(), err)
	}
	for q.Next() {
		var name, typ string
		if err := q.Scan(&name, &typ); err != nil {
			return xerrors.Errorf("failed to scan column of '%s': %w", tableID.String(), err)
		}
		t.colTypes[name] = columntypes.NewTypeDescription(typ)
	}
	if err := q.Err(); err != nil {
		return xerrors.Errorf("failed to read columns of '%s': %w", tableID.String(), err)
	}
	if len(t.colTypes) == 0 {
		// I.e., empty schema could be obtained if table detached or not exists on specific host.
		return xerrors.Errorf("got empty schema for table '%s'", tableID.String())
	}
	return nil
}

func (t *sinkTable) createTable(cols []abstract.ColSchema, distributed bool) error {
	ddl := t.generateDDL(cols, distributed)
	t.logger.Info("DDL start", log.Any("ddl", ddl), log.Any("table", t.tableName))
	if err := t.server.ExecDDL(context.Background(), ddl); err != nil {
		t.logger.Error(fmt.Sprintf("unable to init table:\n%s", ddl), log.Error(err))
		return err
	}
	return nil
}

func (t *sinkTable) generateDDL(cols []abstract.ColSchema, distributed bool) string {
	result := strings.Builder{}
	_, _ = result.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`", t.tableName))
	if distributed {
		_, _ = result.WriteString(fmt.Sprintf(" ON CLUSTER `%s`", t.cluster.topology.ClusterName()))
	}

	columnDefinitions, keyIsNullable := ColumnDefinitions(cols)
	if t.config.IsUpdateable() {
		columnDefinitions = append(columnDefinitions, "`__data_transfer_commit_time` UInt64")
		columnDefinitions = append(columnDefinitions, "`__data_transfer_delete_time` UInt64")
		if t.version.GTE(deleteableVersion) {
			columnDefinitions = append(columnDefinitions, "`__data_transfer_is_deleted` UInt8 MATERIALIZED (if(__data_transfer_delete_time != 0, 1, 0))")
		}
	}
	_, _ = result.WriteString(fmt.Sprintf(" (%s)", strings.Join(columnDefinitions, ", ")))

	engine := "MergeTree"
	engineArgs := make([]string, 0)
	if t.config.IsUpdateable() {
		engine = fmt.Sprintf("Replacing%s", engine)
		engineArgs = append(engineArgs, "__data_transfer_commit_time")
		if t.version.GTE(deleteableVersion) {
			engineArgs = append(engineArgs, "__data_transfer_is_deleted")
		}
	}
	if distributed {
		engine = fmt.Sprintf("Replicated%s", engine)
		engineArgs = append([]string{fmt.Sprintf("'/clickhouse/tables/{shard}/%s.%s_cdc'", t.config.Database(), t.tableName), "'{replica}'"}, engineArgs...)
	}
	result.WriteString(fmt.Sprintf(" ENGINE=%s(%s)", engine, strings.Join(engineArgs, ", ")))

	if keys := keys(cols); len(keys) > 0 {
		escapedKeys := yslices.Map(keys, func(col string) string {
			return fmt.Sprintf("`%s`", col)
		})
		result.WriteString(fmt.Sprintf(" ORDER BY (%s)", strings.Join(escapedKeys, ", ")))
	} else {
		result.WriteString(" ORDER BY tuple()")
	}

	if t.config.Partition() != "" {
		result.WriteString(fmt.Sprintf(" PARTITION BY (%s)", t.config.Partition()))
	}
	if t.config.TTL() != "" {
		result.WriteString(fmt.Sprintf(" TTL %s", t.config.TTL()))
	}
	if keyIsNullable {
		result.WriteString(" SETTINGS allow_nullable_key = 1")
	}

	return result.String()
}

func ColumnDefinitions(cols []abstract.ColSchema) (result []string, keyIsNullable bool) {
	result = make([]string, len(cols))
	keyIsNullable = false
	for i, col := range cols {
		result[i] = chColumnDefinition(&col)
		if isCHNullable(&col) && col.IsKey() {
			keyIsNullable = true
		}
	}
	return result, keyIsNullable
}

func chColumnDefinition(col *abstract.ColSchema) string {
	var chType string
	if origType, ok := getCHOriginalType(col.OriginalType); ok {
		chType = origType
	} else {
		chType = columntypes.ToChType(col.DataType)
		if isCHNullable(col) {
			chType = nullableCHType(chType)
		}
	}
	return fmt.Sprintf("`%s` %s", col.ColumnName, chType)
}

func chColumnDefinitionWithExpression(col *abstract.ColSchema) string {
	var chType string
	if origType, ok := getCHOriginalType(col.OriginalType); ok {
		chType = origType
	} else {
		chType = columntypes.ToChType(col.DataType)
		if isCHNullable(col) {
			chType = nullableCHType(chType)
		}
	}

	var chExpression string
	if _, ok := col.Properties[changeitem.DefaultPropertyKey]; ok {
		chExpression = fmt.Sprintf("DEFAULT CAST(%v, '%s')", col.Properties[changeitem.DefaultPropertyKey], chType)
		return fmt.Sprintf("`%s` %s %s", col.ColumnName, chType, chExpression)
	}
	return fmt.Sprintf("`%s` %s", col.ColumnName, chType)
}

func isCHNullable(col *abstract.ColSchema) bool {
	return !col.Required
}

func nullableCHType(chType string) string {
	return fmt.Sprintf("Nullable(%s)", chType)
}

const originalTypePrefix = "ch:"

func getCHOriginalType(originalType string) (string, bool) {
	isCHOriginal := strings.HasPrefix(originalType, originalTypePrefix)
	if !isCHOriginal {
		return "", false
	}
	res := strings.TrimPrefix(originalType, originalTypePrefix)
	return res, true
}

func keys(schemas []abstract.ColSchema) []string {
	res := make([]string, 0)
	for _, col := range schemas {
		if col.PrimaryKey {
			res = append(res, col.ColumnName)
		}
	}
	return res
}

func (t *sinkTable) ApplyChangeItems(rows []abstract.ChangeItem) error {
	batches := splitRowsBySchema(rows)
	for i, batch := range batches {
		if err := t.applyBatch(batch); err != nil {
			if errors.UpdateToastsError.Contains(err) && t.config.UpsertAbsentToastedRows() {
				t.logger.Warnf("batch insertion fail, fallback to one-by-one pushing (batch #%d)", i)
				for j, batchElem := range batch {
					if err := t.applyBatch([]abstract.ChangeItem{batchElem}); err != nil {
						t.logger.Warnf(
							"Subbatch failed on serial insertion, so previous subbatches as well as previous items in batch will be redelivered (batch #%d, item #%d)",
							i, j,
						)
						return xerrors.Errorf("apply serially element %d of batch %d failed: %w", j, i, err)
					}
				}
			}
			if i != 0 {
				t.logger.Warnf("Subbatch failed, so previous subbatches can be redelivered (batch #%d)", i)
			}
			return xerrors.Errorf("apply batch %d failed: %w", i, err)
		}
	}
	return nil
}

func (t *sinkTable) applyBatch(items []abstract.ChangeItem) error {
	if t.config.MigrationOptions().AddNewColumns {
		if err := t.ApplySchemaDiffToDB(t.cols.Columns(), items[0].TableSchema.Columns()); err != nil {
			return xerrors.Errorf("fail to alter table schema for new batch: %w", err)
		}
		t.cols = items[0].TableSchema
	}
	if t.config.UploadAsJSON() {
		if faultyItem := abstract.FindItemOfKind(items, abstract.UpdateKind, abstract.DeleteKind); faultyItem != nil {
			return abstract.NewFatalError(xerrors.Errorf("ch-sink is configured as Prefer HTTP, but got update/delete changeItem: %s", faultyItem.ToJSONString()))
		}
		return t.uploadAsJSON(items)
	}
	if !t.config.IsUpdateable() {
		if faultyItem := abstract.FindItemOfKind(items, abstract.UpdateKind, abstract.DeleteKind); faultyItem != nil {
			return xerrors.Errorf("ch-sink is configured as not updatable, but got update/delete changeItem: %s", faultyItem.ToJSONString())
		}
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tx, err := t.server.db.BeginTx(ctx, nil)
	if err != nil {
		return xerrors.Errorf("failed to begin transaction: %w", err)
	}
	txRollbacks := util.Rollbacks{}
	txRollbacks.Add(func() {
		if err := tx.Rollback(); err != nil {
			t.logger.Error("transaction rollback error", log.Error(err))
		}
	})
	defer txRollbacks.Do()

	if err := doOperation(t, tx, items); err != nil {
		if errors.IsFatalClickhouseError(err) {
			return abstract.NewFatalError(err)
		}
		return xerrors.Errorf("failed to process change items: %w", err)
	}

	if err := tx.Commit(); err != nil {
		if errors.IsFatalClickhouseError(err) {
			return abstract.NewFatalError(err)
		}
		t.logger.Warn("Commit error", log.Any("ch_host", *t.config.Host()), log.Error(err))
		return xerrors.Errorf("failed to commit: %w", err)
	}
	txRollbacks.Cancel()
	t.metrics.Len.Add(int64(len(items)))
	t.metrics.Count.Inc()

	t.logger.Debugf("Committed %d changeItems (%s) in %v", len(items), *t.config.Host(), time.Since(start))
	return nil
}

func (t *sinkTable) uploadAsJSON(rows []abstract.ChangeItem) error {
	currSchema, err := getSchema(t, rows)
	if err != nil {
		return xerrors.Errorf("Cannot build schema from rows: %w", err)
	}

	if t.avgRowSize == 0 {
		for _, col := range currSchema {
			t.avgRowSize += len(col.ColumnName)
		}
		t.avgRowSize = int(float64(t.avgRowSize) * 1.5)
	}

	st, err := httpuploader2.UploadCIBatch(rows, httpuploader2.NewRules(
		rows[0].ColumnNames,
		currSchema,
		abstract.MakeMapColNameToIndex(currSchema),
		t.colTypes,
		t.config.AnyAsString(),
	), t.config, t.tableName, t.avgRowSize, t.logger)
	if err != nil {
		return err
	}

	rowCnt := len(rows)
	firstOffset, _ := rows[0].Offset()
	lastOffset, _ := rows[rowCnt-1].Offset()
	t.avgRowSize = st.Bytes / rowCnt
	t.logger.Info(
		fmt.Sprintf("Finished [uploadAsJson] %s [%d -> %d] size %s (%d, avg=%s) in (total=%v / upload=%v / marshal=%v)",
			rows[0].Part(),
			firstOffset,
			lastOffset,
			humanize.BigBytes(big.NewInt(int64(st.Bytes))),
			len(rows),
			humanize.Bytes(uint64(st.Bytes/rowCnt)),
			time.Since(st.StartTime),
			time.Since(st.UploadStartTime),
			st.UploadStartTime.Sub(st.StartTime),
		),
		log.Any("size_bytes", st.Bytes),
		log.Any("size_rows", rowCnt),
		log.Any("elapsed_seconds", time.Since(st.StartTime).Seconds()),
	)
	t.metrics.Size.Add(int64(st.Bytes))
	t.metrics.Len.Add(int64(rowCnt))
	t.metrics.Count.Inc()
	return nil
}

// by vals from OldKeys!
func buildDeleteKindArgs(changeItem *abstract.ChangeItem, suffix []interface{}, cols []abstract.ColSchema) []interface{} {
	var args []interface{}
	pkeys := make(map[string]interface{})
	for i, key := range changeItem.OldKeys.KeyNames {
		pkeys[key] = changeItem.OldKeys.KeyValues[i]
	}
	for _, col := range cols {
		if val, ok := pkeys[col.ColumnName]; ok {
			args = append(args, columntypes.Restore(col, val))
		} else {
			if col.Required {
				args = append(args, abstract.DefaultValue(&col))
			} else {
				args = append(args, interface{}(nil))
			}
		}
	}
	args = append(args, suffix...)
	return args
}

func buildChangeItemArgs(changeItem *abstract.ChangeItem, cols []abstract.ColSchema, isUpdatable bool) [][]interface{} {
	var args []interface{}
	if isUpdatable {
		suffixWithDeleteTime := []interface{}{changeItem.CommitTime, changeItem.CommitTime}
		suffixWithoutDeleteTime := []interface{}{changeItem.CommitTime, uint64(0)}

		if changeItem.Kind == abstract.DeleteKind {
			args = buildDeleteKindArgs(changeItem, suffixWithDeleteTime, cols)
		} else if changeItem.KeysChanged() {
			result := make([][]interface{}, 0)
			result = append(result, buildDeleteKindArgs(changeItem, suffixWithDeleteTime, cols))
			result = append(result, append(restoreVals(changeItem.ColumnValues, cols), suffixWithoutDeleteTime...))
			return result
		} else {
			args = append(restoreVals(changeItem.ColumnValues, cols), suffixWithoutDeleteTime...)
		}
	} else {
		args = restoreVals(changeItem.ColumnValues, cols)
	}
	return [][]interface{}{args}
}

func buildColumnNames(schema []abstract.ColSchema) []string {
	colNames := make([]string, len(schema))
	for i, el := range schema {
		colNames[i] = el.ColumnName
	}
	return colNames
}

func equalSchema(tableCols []abstract.ColSchema, currSchema []abstract.ColSchema) error {
	if len(tableCols) != len(currSchema) {
		tableColNames := make([]string, len(tableCols))
		for i, col := range tableCols {
			tableColNames[i] = col.ColumnName
		}
		return abstract.NewFatalError(xerrors.Errorf(
			"len(tableCols) != len(currSchema). changeItemStr: %v, tableColumnsStr: %v",
			currSchema, tableCols))
	}
	for i := 0; i < len(tableCols); i++ {
		if tableCols[i].ColumnName != currSchema[i].ColumnName {
			return abstract.NewFatalError(xerrors.Errorf(
				"tableCols[i].ColumnName != currSchema[i].ColumnName. tableCols[i].ColumnName: %s, currSchema[i].ColumnName: %s, changeItemStr: %v, tableColumnsStr: %v",
				tableCols[i].ColumnName, currSchema[i].ColumnName, currSchema, tableCols))
		}
	}
	return nil
}

func allSchemasEqualTableSchema(t *sinkTable, changeItems []abstract.ChangeItem) error {
	for _, changeItem := range changeItems {
		if changeItem.Kind == abstract.InsertKind || changeItem.Kind == abstract.UpdateKind {
			if err := equalSchema(t.cols.Columns(), changeItem.TableSchema.Columns()); err != nil {
				return err
			}
		}
	}
	return nil
}

func normalizeColumnNamesOrder(changeItems []abstract.ChangeItem) ([]abstract.ChangeItem, error) {
	masterChangeItem := abstract.FindItemOfKind(changeItems, abstract.InsertKind, abstract.UpdateKind)
	if masterChangeItem == nil {
		return changeItems, nil
	}
	colNameToIdx := make(map[string]int)
	for i, el := range masterChangeItem.ColumnNames {
		colNameToIdx[el] = i
	}

	res := changeItems
	rebuild := false
	for _, el := range changeItems {
		if el.Kind == abstract.InsertKind || el.Kind == abstract.UpdateKind {
			if len(el.ColumnNames) != len(masterChangeItem.ColumnNames) {
				return nil, abstract.NewFatalError(xerrors.Errorf("len(el.ColumnNames) != len(masterChangeItem.ColumnNames). masterChangeItem.ColumnNames: %s, el.ColumnNames: %s", masterChangeItem.ColumnNames, el.ColumnNames))
			}
			for i, colName := range el.ColumnNames {
				if idx, ok := colNameToIdx[colName]; ok {
					if i != idx && !rebuild {
						rebuild = true
						logger.Log.Warn(
							"Need to rebuild change item columns",
							log.Any("table", masterChangeItem.Table),
							log.Any("source_colname", el.ColumnNames),
							log.Any("master_colname", masterChangeItem.ColumnNames),
						)
					}
				} else {
					return nil, abstract.NewFatalError(xerrors.Errorf("column %s absent in masterChangeItem. masterChangeItem.ColumnNames: %s", colName, masterChangeItem.ColumnNames))
				}
			}
		}
	}
	if rebuild {
		res = make([]abstract.ChangeItem, len(changeItems))
		for i, el := range changeItems {
			vals := make([]interface{}, len(masterChangeItem.ColumnValues))
			for i, colName := range el.ColumnNames {
				vals[colNameToIdx[colName]] = el.ColumnValues[i]
			}
			res[i] = abstract.ChangeItem{
				ID:           changeItems[i].ID,
				LSN:          changeItems[i].LSN,
				CommitTime:   changeItems[i].CommitTime,
				Counter:      changeItems[i].Counter,
				Kind:         changeItems[i].Kind,
				Schema:       changeItems[i].Schema,
				Table:        changeItems[i].Table,
				PartID:       changeItems[i].PartID,
				ColumnNames:  masterChangeItem.ColumnNames,
				ColumnValues: vals,
				TableSchema:  masterChangeItem.TableSchema,
				OldKeys:      changeItems[i].OldKeys,
				TxID:         changeItems[i].TxID,
				Query:        changeItems[i].Query,
				Size:         changeItems[i].Size,
			}
		}
	}
	return res, nil
}

func columnNamesAllowedSubset(t *sinkTable, masterChangeItem abstract.ChangeItem) error {
	// check if all schema fields in changeItem
	if len(t.cols.Columns()) == len(masterChangeItem.ColumnNames) {
		return nil
	}

	// check if all column_names from changeItem are present in schema
	tableColNameToIdx := make(map[string]int)
	for i, el := range t.cols.Columns() {
		tableColNameToIdx[el.ColumnName] = i
	}
	for _, colName := range masterChangeItem.ColumnNames {
		_, ok := tableColNameToIdx[colName]
		if !ok {
			return abstract.NewFatalError(xerrors.Errorf("column %s in changeItem not present in table schema", colName))
		}
	}

	// check if absent columns in changeItem - virtual
	changeItemColNameToIdx := make(map[string]int)
	for i, colName := range masterChangeItem.ColumnNames {
		changeItemColNameToIdx[colName] = i
	}
	return nil
}

func buildSchemaFromChangeItem(t *sinkTable, masterChangeItem abstract.ChangeItem) []abstract.ColSchema {
	colNameToSchema := make(map[string]abstract.ColSchema)
	for _, el := range t.cols.Columns() {
		colNameToSchema[el.ColumnName] = el
	}
	colSchema := make([]abstract.ColSchema, 0)
	for _, el := range masterChangeItem.ColumnNames {
		col := colNameToSchema[el]
		// clickhouse-go v2 requires exactly the same type as declared in the table schema
		// So lets use this type in col schema thus making chcommon.Restore to convert raw value to the correct type
		// See TM-5198 and related tickets for details
		if colType, ok := t.colTypes[el]; ok && colType.IsInteger {
			col.DataType = colType.YtBaseType
		}
		colSchema = append(colSchema, col)
	}
	return colSchema
}

func getSchema(t *sinkTable, changeItems []abstract.ChangeItem) ([]abstract.ColSchema, error) {
	masterChangeItem := abstract.FindItemOfKind(changeItems, abstract.InsertKind, abstract.UpdateKind)

	if masterChangeItem == nil {
		return t.cols.Columns(), nil
	}
	if err := allSchemasEqualTableSchema(t, changeItems); err != nil {
		//nolint:descriptiveerrors
		return nil, err
	}
	if err := columnNamesAllowedSubset(t, *masterChangeItem); err != nil {
		//nolint:descriptiveerrors
		return nil, err
	}
	return buildSchemaFromChangeItem(t, *masterChangeItem), nil
}

func doOperation(t *sinkTable, tx *sql.Tx, items []abstract.ChangeItem) (err error) {
	if len(items) == 0 {
		return nil
	}

	defer func() {
		// clickhouse-go/v2 driver sometimes may panic on reflection
		if r := recover(); r != nil {
			err = abstract.NewFatalError(xerrors.Errorf("panic inside clickhouse sink: %v", r))
			logger.Log.Error("Clickhouse sink panic", log.Error(err), log.String("stacktrace", string(debug.Stack())))
		}
	}()

	if abstract.FindItemOfKind(items, abstract.UpdateKind) != nil {
		items = abstract.Collapse(items)
		normalItems, err := convertToastedToNormal(t, items)
		if err != nil {
			return xerrors.Errorf("failed to convert toasted items to normal ones: %w", err)
		}
		items = normalItems
	}

	items, err = normalizeColumnNamesOrder(items)
	if err != nil {
		return xerrors.Errorf("unable to normalize column names order for table %q: %w", t.tableName, err)
	}
	currSchema, err := getSchema(t, items)
	if err != nil {
		return xerrors.Errorf("unable to get schema for table %q: %w", t.tableName, err)
	}

	colNames := make([]string, len(currSchema))
	colVals := make([]string, len(currSchema))
	for idx, colSchema := range currSchema {
		colNames[idx] = fmt.Sprintf("`%s`", colSchema.ColumnName)
		colVals[idx] = "?"
	}

	if t.config.IsUpdateable() {
		colNames = append(colNames, "`__data_transfer_commit_time`")
		colVals = append(colVals, "?")
		colNames = append(colNames, "`__data_transfer_delete_time`")
		colVals = append(colVals, "?")
	}

	q := fmt.Sprintf(
		"INSERT INTO `%s`.`%s` (%s) VALUES (%s)",
		t.config.Database(),
		t.tableName,
		strings.Join(colNames, ","),
		strings.Join(colVals, ","),
	)

	insertCtx := clickhouse.Context(context.Background(), t.config.InsertSettings().ToQueryOption())
	insertQuery, err := tx.PrepareContext(insertCtx, q)
	if err != nil {
		if err.Error() == "Decimal128 is not supported" {
			return abstract.NewFatalError(xerrors.New("Decimal128 is not supported by native clickhouse-go driver. try to switch on HTTP/JSON protocol in dst endpoint settings"))
		}
		return xerrors.Errorf("unable to prepare change item for table %q: %s: %w", t.tableName, q, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	for i := range items {
		// TODO - handle AlterTable
		argsArr := buildChangeItemArgs(&items[i], currSchema, t.config.IsUpdateable())
		for _, args := range argsArr {
			if _, err := insertQuery.ExecContext(ctx, args...); err != nil {
				t.logger.Error("Unable to exec changeItem", log.Error(err))
				if xerrors.Is(err, new(column.ColumnConverterError)) {
					return abstract.NewFatalError(xerrors.Errorf("Unable to exec changeItem (converter error): %w", err))
				}
				return xerrors.Errorf("Unable to exec changeItem: %w", err)
			}
		}
	}

	return err
}

func (t *sinkTable) checkExist() (bool, error) {
	exist := false
	err := t.server.db.
		QueryRow(`select count() > 0 from system.tables where name = ? and database = ?;`, t.tableName, t.config.Database()).
		Scan(&exist)
	if err != nil {
		return exist, err
	}
	return exist, nil
}

func restoreVals(vals []interface{}, cols []abstract.ColSchema) []interface{} {
	res := make([]interface{}, len(vals))
	for i := range vals {
		res[i] = columntypes.Restore(cols[i], vals[i])
	}
	return res
}

func (t *sinkTable) ApplySchemaDiffToDB(oldSchema []abstract.ColSchema, newSchema []abstract.ColSchema) error {
	added, removed := compareColumnSets(oldSchema, newSchema)
	if len(removed) != 0 {
		removedNames := make([]string, 0, len(removed))
		for _, col := range removed {
			removedNames = append(removedNames, col.ColumnName)
		}
		t.logger.Warnf("Some columns missed: %s. Hope, it's ok", strings.Join(removedNames, ","))
	}
	if len(added) == 0 {
		return nil
	}
	return t.cluster.execDDL(func(distributed bool) error {
		return t.alterTable(added, nil, distributed)
	})
}

func splitRowsBySchema(rows []abstract.ChangeItem) [][]abstract.ChangeItem {
	currentSchema := rows[0].TableSchema.Columns()
	batches := make([][]abstract.ChangeItem, 0, 2)
	batch := make([]abstract.ChangeItem, 0, len(rows))

	for i, row := range rows {
		if row.Kind == abstract.InsertKind {
			row = patchSchemaForOutdatedInsert(row)
		}
		if err := equalSchema(currentSchema, row.TableSchema.Columns()); err != nil {
			if len(batch) != 0 {
				batches = append(batches, batch)
				batch = make([]abstract.ChangeItem, 0, len(rows)-i)
			}
		}
		batch = append(batch, row)
		currentSchema = row.TableSchema.Columns()
	}
	batches = append(batches, batch)
	return batches
}

func patchSchemaForOutdatedInsert(insertItem abstract.ChangeItem) abstract.ChangeItem {
	// If INSERT changeItem has absent value for non-virtual column, column was actually removed on source
	if len(insertItem.TableSchema.Columns()) == len(insertItem.ColumnNames) {
		return insertItem
	}

	changeItemCols := make(map[string]bool)
	for _, colName := range insertItem.ColumnNames {
		changeItemCols[colName] = true
	}

	colsToDelete := make(map[int]bool)
	for i, el := range insertItem.TableSchema.Columns() {
		ok := changeItemCols[el.ColumnName]
		if !ok && !IsColVirtual(el) {
			colsToDelete[i] = true
		}
	}

	realTableSchema := make([]abstract.ColSchema, 0, len(insertItem.TableSchema.Columns())-len(colsToDelete))
	for i, col := range insertItem.TableSchema.Columns() {
		if del := colsToDelete[i]; !del {
			realTableSchema = append(realTableSchema, col)
		}
	}
	insertItem.SetTableSchema(abstract.NewTableSchema(realTableSchema))
	return insertItem
}

func compareColumnSets(currentSchema []abstract.ColSchema, newSchema []abstract.ColSchema) (added, removed []abstract.ColSchema) {
	currentColSet := make(map[string]bool)
	for _, col := range currentSchema {
		currentColSet[col.ColumnName] = true
	}
	newColSet := make(map[string]bool)
	for _, col := range newSchema {
		newColSet[col.ColumnName] = true
	}

	for _, col := range currentSchema {
		if ok := newColSet[col.ColumnName]; !ok {
			removed = append(removed, col)
		}
	}
	for _, col := range newSchema {
		if ok := currentColSet[col.ColumnName]; !ok {
			added = append(added, col)
		}
	}
	return added, removed
}

func (t *sinkTable) alterTable(addCols, dropCols []abstract.ColSchema, distributed bool) error {
	ddl := fmt.Sprintf("ALTER TABLE `%s` ", t.tableName)
	if distributed {
		ddl += fmt.Sprintf(" ON CLUSTER `%s` ", t.cluster.topology.ClusterName())
	}

	ddlItems := make([]string, 0, len(addCols)+len(dropCols))
	for _, col := range addCols {
		ddlItems = append(ddlItems, fmt.Sprintf("ADD COLUMN IF NOT EXISTS %s", chColumnDefinitionWithExpression(&col)))
	}
	for _, col := range dropCols {
		ddlItems = append(ddlItems, fmt.Sprintf("DROP COLUMN IF EXISTS `%s`", col.ColumnName))
	}
	ddl += strings.Join(ddlItems, ", ")

	t.logger.Info("ALTER DDL start", log.Any("ddl", ddl), log.Any("table", t.tableName))
	if err := t.server.ExecDDL(context.Background(), ddl); err != nil {
		t.logger.Error(fmt.Sprintf("Unable to alter table:\n%s", ddl), log.Error(err))
		return err
	}
	if err := t.fillColNameToType(); err != nil {
		return xerrors.Errorf("failed to infer columns: %w", err)
	}
	return nil
}
