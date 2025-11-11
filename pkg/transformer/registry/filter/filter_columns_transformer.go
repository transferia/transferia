package filter

import (
	"fmt"
	"strings"
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/errors/coded"
	"github.com/transferia/transferia/pkg/errors/codes"
	"github.com/transferia/transferia/pkg/transformer"
	"github.com/transferia/transferia/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
)

const FilterColumnsTransformerType = abstract.TransformerType("filter_columns")

func init() {
	transformer.Register[FilterColumnsConfig](
		FilterColumnsTransformerType,
		func(protoConfig FilterColumnsConfig, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
			return NewFilterColumnsTransformer(protoConfig, lgr)
		},
	)
}

type FilterColumnsConfig struct {
	Columns Columns `json:"columns"`
	Tables  Tables  `json:"tables"`
}

type FilterColumnsTransformer struct {
	Columns Filter
	Tables  Filter
	Logger  log.Logger

	cacheMu sync.RWMutex
	cache   map[abstract.TableID]map[string]*filteredTableSchema
}

type filteredTableSchema struct {
	ColNames set.Set[string]
	Columns  []abstract.ColSchema
}

func (f *FilterColumnsTransformer) Type() abstract.TransformerType {
	return FilterColumnsTransformerType
}

func (f *FilterColumnsTransformer) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	transformed := make([]abstract.ChangeItem, 0)
	errors := make([]abstract.TransformerError, 0)
	for _, item := range input {
		valueIndexes, filteredSchema, err := f.filterValueIndexes(item)
		if err != nil {
			f.Logger.Errorf("unable to filter columns indexes: %v", err)
			errors = append(errors, abstract.TransformerError{
				Input: item,
				Error: err,
			})
			continue
		}

		if result, err := f.trimChangeItem(item, valueIndexes, filteredSchema); err == nil {
			transformed = append(transformed, result)
		} else {
			f.Logger.Errorf("unable to trim change item: %v", err)
			errors = append(errors, abstract.TransformerError{
				Input: item,
				Error: err,
			})
		}
	}
	return abstract.TransformerResult{
		Transformed: transformed,
		Errors:      errors,
	}
}

func (f *FilterColumnsTransformer) filterValueIndexes(item abstract.ChangeItem) ([]int, *filteredTableSchema, error) {
	schema, err := f.getFilteredSchema(item.TableID(), item.TableSchema)
	if err != nil {
		return nil, nil, xerrors.Errorf("cannot filter columns for table %v: %w", item.Fqtn(), err)
	}
	indexes, err := f.getColumnIndexes(item, schema.ColNames)
	return indexes, schema, err
}

func (f *FilterColumnsTransformer) getFilteredSchema(tID abstract.TableID, schema *abstract.TableSchema) (*filteredTableSchema, error) {
	hsh, err := schema.Hash()
	if err != nil {
		f.Logger.Warnf("getting schema hash failed, thus cannot use cache for schema: %v", err)
		return f.newFilteredTableSchema(schema)
	}
	sch, ok := f.getCachedSchema(tID, hsh)
	if ok {
		return sch, nil
	}
	sch, err = f.cacheSchema(tID, schema)
	if err != nil {
		return nil, xerrors.Errorf("cannot make filtered schema: %w", err)
	}
	return sch, nil
}

func (f *FilterColumnsTransformer) newFilteredTableSchema(schema *abstract.TableSchema) (*filteredTableSchema, error) {
	colsNames := set.New[string]([]string{}...)
	colsSchema := []abstract.ColSchema{}
	for _, c := range schema.Columns() {
		if !f.Columns.Match(c.ColumnName) {
			if c.PrimaryKey {
				return nil, xerrors.Errorf("cannot exclude primary key column %v", c.ColumnName)
			}
		} else {
			colsNames.Add(c.ColumnName)
			colsSchema = append(colsSchema, c)
		}
	}

	return &filteredTableSchema{
		ColNames: *colsNames,
		Columns:  colsSchema,
	}, nil
}

func (f *FilterColumnsTransformer) getCachedSchema(tID abstract.TableID, hsh string) (*filteredTableSchema, bool) {
	f.cacheMu.RLock()
	defer f.cacheMu.RUnlock()

	tableCache, ok := f.cache[tID]
	if !ok {
		return nil, false
	}
	sch, ok := tableCache[hsh]
	return sch, ok
}

func (f *FilterColumnsTransformer) cacheSchema(tID abstract.TableID, schema *abstract.TableSchema) (*filteredTableSchema, error) {
	hsh, err := schema.Hash()
	if err != nil {
		return nil, xerrors.Errorf("cannot get table schema hash: %w", err)
	}
	filteredSchema, err := f.newFilteredTableSchema(schema)
	if err != nil {
		return nil, xerrors.Errorf("cannot filter columns in table %v: %w", tID.Fqtn(), err)
	}
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	if f.cache[tID] == nil {
		f.cache[tID] = make(map[string]*filteredTableSchema)
	}
	f.cache[tID][hsh] = filteredSchema
	return filteredSchema, nil
}

func (f *FilterColumnsTransformer) getColumnIndexes(item abstract.ChangeItem, filteredColumns set.Set[string]) ([]int, error) {
	indexes := make([]int, 0)
	for i, name := range item.ColumnNames {
		if filteredColumns.Contains(name) {
			indexes = append(indexes, i)
		}
	}
	return indexes, nil
}

func (f *FilterColumnsTransformer) trimChangeItem(original abstract.ChangeItem, filteredColsIndexes []int, filteredSchema *filteredTableSchema) (abstract.ChangeItem, error) {
	filteredColumnsCount := len(filteredColsIndexes)
	if len(original.ColumnNames) == filteredColumnsCount && len(filteredSchema.Columns) == len(original.TableSchema.Columns()) {
		return original, nil
	}

	transformed := original
	transformed.ColumnNames = make([]string, filteredColumnsCount)
	transformed.ColumnValues = make([]interface{}, filteredColumnsCount)
	transformed.SetTableSchema(abstract.NewTableSchema(filteredSchema.Columns))

	for newIndex, origIndex := range filteredColsIndexes {
		transformed.ColumnNames[newIndex] = original.ColumnNames[origIndex]
		transformed.ColumnValues[newIndex] = original.ColumnValues[origIndex]
	}

	transformed.OldKeys = copyAndTrimOldKeys(&original.OldKeys, filteredSchema.ColNames)
	return transformed, nil
}

func copyAndTrimOldKeys(oldKeys *abstract.OldKeysType, filteredColumns set.Set[string]) abstract.OldKeysType {
	newLen := 0
	for _, keyName := range oldKeys.KeyNames {
		if filteredColumns.Contains(keyName) {
			newLen++
		}
	}
	if newLen == len(oldKeys.KeyNames) {
		return copyOldKeys(oldKeys)
	}
	trimmedOldKeys := abstract.OldKeysType{
		KeyNames:  make([]string, 0, newLen),
		KeyTypes:  make([]string, 0, newLen),
		KeyValues: make([]interface{}, 0, newLen),
	}
	for i, keyName := range oldKeys.KeyNames {
		if !filteredColumns.Contains(keyName) {
			continue
		}
		trimmedOldKeys.KeyNames = append(trimmedOldKeys.KeyNames, oldKeys.KeyNames[i])
		trimmedOldKeys.KeyValues = append(trimmedOldKeys.KeyValues, oldKeys.KeyValues[i])
		if len(oldKeys.KeyTypes) == len(oldKeys.KeyNames) {
			trimmedOldKeys.KeyTypes = append(trimmedOldKeys.KeyTypes, oldKeys.KeyTypes[i])
		}
	}
	return trimmedOldKeys
}

func (f *FilterColumnsTransformer) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	return MatchAnyTableNameVariant(f.Tables, table) && f.validSchema(schema.Columns())
}

func (f *FilterColumnsTransformer) validSchema(columns abstract.TableColumns) bool {
	for _, col := range columns {
		if !f.Columns.Match(col.ColumnName) && col.PrimaryKey {
			return false
		}
	}
	return true
}

func (f *FilterColumnsTransformer) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	result := make([]abstract.ColSchema, 0)
	for _, col := range original.Columns() {
		if f.Columns.Match(col.ColumnName) {
			result = append(result, col)
		}
	}
	return abstract.NewTableSchema(result), nil
}

func (f *FilterColumnsTransformer) Description() string {
	includeStr := trimStr(strings.Join(f.Columns.IncludeRegexp, "|"), 100)
	excludeStr := trimStr(strings.Join(f.Columns.ExcludeRegexp, "|"), 100)
	return fmt.Sprintf("Filter columns: include: %v, exclude: %v", includeStr, excludeStr)
}

func trimStr(value string, maxLength int) string {
	if len(value) > maxLength {
		value = value[:maxLength]
	}
	return value
}

func NewFilterColumnsTransformer(config FilterColumnsConfig, lgr log.Logger) (*FilterColumnsTransformer, error) {
	var tables Filter
	var err error
	if config.Tables.ExcludeTables != nil || config.Tables.IncludeTables != nil {
		tables, err = NewFilter(config.Tables.IncludeTables, config.Tables.ExcludeTables)
	} else {
		tables, err = NewFilter(nil, nil)
	}
	if err != nil {
		return nil, xerrors.Errorf("unable to init tables filter: %w", err)
	}

	if config.Columns.IncludeColumns == nil && config.Columns.ExcludeColumns == nil {
		return nil, coded.Errorf(codes.FilterColumnsEmpty, "filter for columns cannot be empty in user defined transformation")
	}

	colFilter, err := NewFilter(config.Columns.IncludeColumns, config.Columns.ExcludeColumns)
	if err != nil {
		return nil, xerrors.Errorf("unable to init columns transformer: %w", err)
	}
	return NewCustomFilterColumnsTransformer(tables, colFilter, lgr), nil
}

func NewCustomFilterColumnsTransformer(tables, columns Filter, lgr log.Logger) *FilterColumnsTransformer {
	return &FilterColumnsTransformer{
		Tables:  tables,
		Columns: columns,
		Logger:  lgr,
		cacheMu: sync.RWMutex{},
		cache:   make(map[abstract.TableID]map[string]*filteredTableSchema),
	}
}
