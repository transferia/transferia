//go:build !disable_clickhouse_provider

package clickhouse

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/errors/coded"
	"github.com/transferia/transferia/pkg/providers/clickhouse/columntypes"
	"github.com/transferia/transferia/pkg/providers/clickhouse/errors"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

const verColumnName = "__data_transfer_commit_time"

func getToastedChangeItems(t *sinkTable, changeItems []abstract.ChangeItem) ([]abstract.ChangeItem, error) {
	tableColNameToIdx := t.cols.FastColumns()

	virtualColsNum := 0
	for _, el := range t.cols.Columns() {
		if IsColVirtual(el) {
			virtualColsNum++
		}
	}

	result := make([]abstract.ChangeItem, 0)
	for _, currChangeItem := range changeItems {
		if currChangeItem.Kind != abstract.UpdateKind {
			continue
		}
		for _, colName := range currChangeItem.ColumnNames {
			_, ok := tableColNameToIdx[abstract.ColumnName(colName)]
			if !ok {
				return nil, abstract.NewFatalError(xerrors.Errorf("column %s is unknown to target scheme", colName))
			}
		}
		if len(t.cols.Columns()) > len(currChangeItem.ColumnNames)+virtualColsNum {
			t.logger.Debugf("Found differing column lengths for toasted items, on dst: %v, on source: %v", len(t.cols.Columns()), len(currChangeItem.ColumnNames)+virtualColsNum)
			t.logger.Debugf("The columns for toasted items are on dst: %v, on source: %v", t.cols.Columns(), currChangeItem.ColumnNames)
			result = append(result, currChangeItem)
		}
	}

	return result, nil
}

func buildPkeysTemplate(cols []abstract.ColSchema) string {
	q := make([]string, 0)
	for _, col := range cols {
		if col.IsKey() {
			q = append(q, fmt.Sprintf("%s=?", col.ColumnName))
		}
	}
	return "(" + strings.Join(q, " AND ") + ")"
}

func buildPkeysDistinct(cols []abstract.ColSchema) string {
	q := make([]string, 0)
	for _, col := range cols {
		if col.IsKey() {
			q = append(q, col.ColumnName)
		}
	}
	return "(" + strings.Join(q, ",") + ")"
}

// primaryKeyValuesFlattened returns a list of (old) primary key values for all items in the given list
func primaryKeyValuesFlattened(items []abstract.ChangeItem, colSchemas []abstract.ColSchema) ([]interface{}, error) {
	colSchemaByName := make(map[string]*abstract.ColSchema)
	for i := range colSchemas {
		colSchemaByName[colSchemas[i].ColumnName] = &colSchemas[i]
	}

	result := make([]interface{}, 0)
	for _, item := range items {
		for i, keyName := range item.OldKeys.KeyNames {
			keyColSchema, ok := colSchemaByName[keyName]
			if !ok {
				return nil, abstract.NewFatalError(xerrors.Errorf("schema for key column %q not found", keyName))
			}

			if !keyColSchema.IsKey() {
				continue
			}

			restoredKeyVal := columntypes.Restore(*keyColSchema, item.OldKeys.KeyValues[i])
			if restoredKeyVal == nil {
				return nil, abstract.NewFatalError(xerrors.Errorf("Value of key column %q not found. This can happen if you use toasted values as keys", keyName))
			}
			result = append(result, restoredKeyVal)
		}
	}

	return result, nil
}

func getColumns(cols []abstract.ColSchema, changeItems []abstract.ChangeItem) ([]abstract.ColSchema, error) {
	colNameToIdx := make(map[string]int)
	for i, col := range cols {
		colNameToIdx[col.ColumnName] = i
	}

	colNameToCount := make(map[string]int)
	for _, changeItem := range changeItems {
		for _, colName := range changeItem.ColumnNames {
			colNameToCount[colName]++
		}
	}
	for _, col := range cols {
		if col.PrimaryKey {
			colNameToCount[col.ColumnName] = 0 // to include pkeys
		}

		if _, ok := colNameToCount[col.ColumnName]; !ok {
			colNameToCount[col.ColumnName] = 0 // to consider columns who absent in changeItems
		}
	}

	result := make([]abstract.ColSchema, 0)
	for colName, count := range colNameToCount {
		idx, ok := colNameToIdx[colName]
		if !ok {
			return nil, abstract.NewFatalError(xerrors.Errorf("column %s is unknown to target scheme", colName))
		}
		if count != len(changeItems) {
			result = append(result, cols[idx])
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ColumnName < result[j].ColumnName
	})

	return result, nil
}

// convertToastedToNormal converts toasted items (if any are present) to their complete versions using values ofbtained from the target CH
func convertToastedToNormal(t *sinkTable, items []abstract.ChangeItem) ([]abstract.ChangeItem, error) {
	toastedItems, err := getToastedChangeItems(t, items)
	if err != nil {
		return nil, xerrors.Errorf("failed to separate toasted and normal items in the incoming batch of %d items: %w", len(items), err)
	}
	if len(toastedItems) == 0 {
		return items, nil
	}

	lastVersionFullRows, err := fetchToastedRows(t, toastedItems)
	if err != nil {
		if len(items) == 1 && t.config.UpsertAbsentToastedRows() {
			t.logger.Info("cannot extract TOAST for change item, but since UpsertAbsentToastedRows is turned on -- skip toasting",
				log.Any("table", items[0].TableID()))
			return items, nil
		}
		return nil, coded.Errorf(errors.UpdateToastsError, "failed to extract previous versions for %d toasted items: %w", len(toastedItems), err)
	}
	t.logger.Info("previous versions of toasted items extracted successfully", log.Int("len", len(lastVersionFullRows)))

	return abstract.Collapse(append(lastVersionFullRows, items...)), nil // to merge TOASTed rows with absent values
}

func fetchToastedRows(t *sinkTable, changeItems []abstract.ChangeItem) ([]abstract.ChangeItem, error) {
	tableName := changeItems[0].Table
	schemaName := changeItems[0].Schema

	keyCols := changeItems[0].MakeMapKeys()
	keyToIdx := make(map[string]int)
	for i := range changeItems {
		hashK := pKHash(t, changeItems[i], keyCols)
		keyToIdx[hashK] = i
	}

	filteredColumns, err := getColumns(t.cols.Columns(), changeItems)
	if err != nil {
		return nil, xerrors.Errorf("failed to build schema: %w", err)
	}
	columnNames := buildColumnNames(filteredColumns)
	columnNamesStr := strings.Join(columnNames, ",")
	pkeysTemplate := buildPkeysTemplate(t.cols.Columns())
	pkeysTemplateArr := make([]string, 0)
	for range changeItems {
		pkeysTemplateArr = append(pkeysTemplateArr, pkeysTemplate)
	}
	pkeysTemplateStr := strings.Join(pkeysTemplateArr, " OR ")

	limitKeys := buildPkeysDistinct(t.cols.Columns())
	queryTemplate := fmt.Sprintf(
		"SELECT %s FROM `%s`.`%s` WHERE %s ORDER BY %s DESC LIMIT 1 BY %s",
		columnNamesStr,
		t.config.Database(),
		t.tableName,
		pkeysTemplateStr,
		verColumnName,
		limitKeys,
	)

	pkValues, err := primaryKeyValuesFlattened(changeItems, t.cols.Columns())
	if err != nil {
		return nil, xerrors.Errorf("failed to calculate primary key values required for %q of toasted values: %w", queryTemplate, err)
	}

	pkValues = removePKTimezoneInfo(t, pkValues)

	queryResult, err := t.server.db.Query(queryTemplate, pkValues...)
	if err != nil {
		return nil, xerrors.Errorf("failed to execute %q for toasted values: %w", queryTemplate, err)
	}
	defer queryResult.Close()
	rowValues, err := InitValuesForScan(queryResult)
	if err != nil {
		return nil, xerrors.Errorf("unable to init values for scan result: %w", err)
	}

	result := make([]abstract.ChangeItem, 0)
	for queryResult.Next() {

		err := queryResult.Scan(rowValues...)
		if err != nil {
			return nil, xerrors.Errorf("unable to scan row from result: %w", err)
		}

		vals, _ := MarshalFields(rowValues, filteredColumns)
		changeItem := abstract.ChangeItem{
			ID:           0,
			LSN:          0,
			CommitTime:   0,
			Counter:      0,
			Kind:         abstract.InsertKind,
			Schema:       schemaName,
			Table:        tableName,
			PartID:       "",
			ColumnNames:  columnNames,
			ColumnValues: vals,
			TableSchema:  t.cols,
			OldKeys: abstract.OldKeysType{
				KeyNames:  nil,
				KeyTypes:  nil,
				KeyValues: nil,
			},
			TxID:  "",
			Query: "",
			Size:  abstract.RawEventSize(util.DeepSizeof(rowValues)),
		}

		hashK := pKHash(t, changeItem, keyCols)
		i, ok := keyToIdx[hashK]
		if !ok {
			return nil, xerrors.Errorf("unknown pkeys: %s", hashK)
		}

		changeItem.ID = changeItems[i].ID
		changeItem.LSN = changeItems[i].LSN
		changeItem.CommitTime = changeItems[i].CommitTime
		changeItem.Counter = changeItems[i].Counter
		changeItem.TxID = changeItems[i].TxID
		changeItem.Query = changeItems[i].Query

		result = append(result, changeItem)
	}

	if err := queryResult.Err(); err != nil {
		return nil, xerrors.Errorf("fail during iteration over query result: %w", err)
	}

	if len(result) != len(changeItems) {
		return nil, xerrors.Errorf("%q from the destination returned a different number of items (%d) than the number of incoming toasted items (%d)", queryTemplate, len(result), len(changeItems))
	}

	return result, nil
}

func removePKTimezoneInfo(t *sinkTable, pkValues []interface{}) []interface{} {
	var pkVals []interface{}
	for _, pkVal := range pkValues {
		pkVals = append(pkVals, dropTimezone(t, pkVal))
	}

	return pkVals
}

// dropTimezone ensures that an incoming value of type time.Time does not contain any timezone info
// The value itself is also converted to the cluster timezone in order for timestamps to match on CH side.
// Underlying data type is switched from time.Time -> string.
func dropTimezone(t *sinkTable, pkVal interface{}) interface{} {
	timestamp, ok := pkVal.(time.Time)
	if !ok {
		return pkVal // leave as is
	}

	val := timestamp.UTC().In(t.timezone)
	timeElements := strings.Split(val.String(), " ")
	if len(timeElements) < 2 {
		return pkVal
	}
	noTimezoneTimestamp := fmt.Sprintf("%s %s", timeElements[0], timeElements[1])
	return noTimezoneTimestamp
}

func pKHash(t *sinkTable, item abstract.ChangeItem, keyColumns map[string]bool) string {
	keys := make(map[string]interface{})
	for k := range keyColumns {
		keys[k] = nil
	}

	if (item.Kind == abstract.UpdateKind || item.Kind == abstract.DeleteKind) && len(item.OldKeys.KeyValues) > 0 {
		for i, keyName := range item.OldKeys.KeyNames {
			if keyColumns[keyName] {
				val := item.OldKeys.KeyValues[i]
				keys[keyName] = dropTimezone(t, val)
			}
		}
	} else {
		for i, colName := range item.ColumnNames {
			if keyColumns[colName] {
				val := item.ColumnValues[i]
				keys[colName] = dropTimezone(t, val)
			}
		}
	}

	toMarshal := make([]interface{}, len(keys))
	for i, colName := range util.MapKeysInOrder(keys) {
		toMarshal[i] = keys[colName]
	}

	d, _ := json.Marshal(toMarshal)
	return string(d)
}
