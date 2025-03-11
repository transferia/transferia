package helpers

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
)

type ChangeItemsBuilder struct {
	Schema         string
	Table          string
	TableSchema    *abstract.TableSchema
	colNameToIdx   map[string]int
	numPrimaryKeys int
}

func (c *ChangeItemsBuilder) Inserts(t *testing.T, in []map[string]interface{}) []abstract.ChangeItem {
	result := make([]abstract.ChangeItem, 0)
	for _, el := range in {
		currElem := abstract.ChangeItem{
			Kind:         abstract.InsertKind,
			Schema:       c.Schema,
			Table:        c.Table,
			ColumnNames:  make([]string, 0),
			ColumnValues: make([]interface{}, 0),
			TableSchema:  c.TableSchema,
		}
		for k, v := range el {
			if _, ok := c.colNameToIdx[k]; !ok {
				require.FailNow(t, "didn't find column name: %s", k)
			}
			currElem.ColumnNames = append(currElem.ColumnNames, k)
			currElem.ColumnValues = append(currElem.ColumnValues, v)
		}
		result = append(result, currElem)
	}
	return result
}

func (c *ChangeItemsBuilder) Updates(t *testing.T, in []map[string]interface{}, oldKeys []map[string]interface{}) []abstract.ChangeItem {
	result := make([]abstract.ChangeItem, 0)
	require.Equal(t, len(in), len(oldKeys))
	for index := range in {
		newVals := in[index]
		currOldKeys := oldKeys[index]

		currElem := abstract.ChangeItem{
			Kind:         abstract.UpdateKind,
			Schema:       c.Schema,
			Table:        c.Table,
			ColumnNames:  make([]string, 0),
			ColumnValues: make([]interface{}, 0),
			TableSchema:  c.TableSchema,
			OldKeys: abstract.OldKeysType{
				KeyNames:  make([]string, 0),
				KeyTypes:  nil,
				KeyValues: make([]interface{}, 0),
			},
		}
		for k, v := range newVals {
			if _, ok := c.colNameToIdx[k]; !ok {
				require.FailNow(t, "didn't find column name: %s", k)
			}
			currElem.ColumnNames = append(currElem.ColumnNames, k)
			currElem.ColumnValues = append(currElem.ColumnValues, v)
		}
		numPrimaryKeys := 0
		for k, v := range currOldKeys {
			if _, ok := c.colNameToIdx[k]; !ok {
				require.FailNow(t, "didn't find column name: %s", k)
			}
			index := c.colNameToIdx[k]
			if c.TableSchema.Columns()[index].PrimaryKey {
				numPrimaryKeys++
				currElem.OldKeys.KeyNames = append(currElem.OldKeys.KeyNames, k)
				currElem.OldKeys.KeyValues = append(currElem.OldKeys.KeyValues, v)
			}
		}
		if numPrimaryKeys != c.numPrimaryKeys {
			require.FailNow(t, "numPrimaryKeys: %d, c.numPrimaryKeys: %v", numPrimaryKeys, c.numPrimaryKeys)
		}
		result = append(result, currElem)
	}
	return result
}

func (c *ChangeItemsBuilder) Deletes(t *testing.T, oldKeys []map[string]interface{}) []abstract.ChangeItem {
	result := make([]abstract.ChangeItem, 0)
	for _, el := range oldKeys {
		currElem := abstract.ChangeItem{
			Kind:         abstract.DeleteKind,
			Schema:       c.Schema,
			Table:        c.Table,
			ColumnNames:  make([]string, 0),
			ColumnValues: make([]interface{}, 0),
			TableSchema:  c.TableSchema,
			OldKeys: abstract.OldKeysType{
				KeyNames:  make([]string, 0),
				KeyTypes:  nil,
				KeyValues: make([]interface{}, 0),
			},
		}
		for k, v := range el {
			if _, ok := c.colNameToIdx[k]; !ok {
				require.FailNow(t, "didn't find column name: %s", k)
			}
			index := c.colNameToIdx[k]
			if c.TableSchema.Columns()[index].PrimaryKey {
				currElem.OldKeys.KeyNames = append(currElem.OldKeys.KeyNames, k)
				currElem.OldKeys.KeyValues = append(currElem.OldKeys.KeyValues, v)
			}
		}
		if len(currElem.OldKeys.KeyNames) != c.numPrimaryKeys {
			require.FailNow(t, "len(currElem.OldKeys.KeyNames): %d, c.numPrimaryKeys: %v", len(currElem.OldKeys.KeyNames), c.numPrimaryKeys)
		}
		result = append(result, currElem)
	}
	return result
}

func (c *ChangeItemsBuilder) InitShardedTableLoad() []abstract.ChangeItem {
	return []abstract.ChangeItem{
		{
			Kind:        abstract.InitShardedTableLoad,
			Schema:      c.Schema,
			Table:       c.Table,
			TableSchema: c.TableSchema,
		},
	}
}

func (c *ChangeItemsBuilder) InitTableLoad() []abstract.ChangeItem {
	return []abstract.ChangeItem{
		{
			Kind:        abstract.InitTableLoad,
			Schema:      c.Schema,
			Table:       c.Table,
			TableSchema: c.TableSchema,
		},
	}
}

func (c *ChangeItemsBuilder) DoneTableLoad() []abstract.ChangeItem {
	return []abstract.ChangeItem{
		{
			Kind:        abstract.DoneTableLoad,
			Schema:      c.Schema,
			Table:       c.Table,
			TableSchema: c.TableSchema,
		},
	}
}

func (c *ChangeItemsBuilder) DoneShardedTableLoad() []abstract.ChangeItem {
	return []abstract.ChangeItem{
		{
			Kind:        abstract.DoneShardedTableLoad,
			Schema:      c.Schema,
			Table:       c.Table,
			TableSchema: c.TableSchema,
		},
	}
}

func NewChangeItemsBuilder(
	schema string,
	table string,
	tableSchema *abstract.TableSchema,
) ChangeItemsBuilder {
	colNameToIdx := make(map[string]int)
	numPrimaryKeys := 0
	for index, el := range tableSchema.Columns() {
		colNameToIdx[el.ColumnName] = index
		if el.PrimaryKey {
			numPrimaryKeys++
		}
	}
	return ChangeItemsBuilder{
		Schema:         schema,
		Table:          table,
		TableSchema:    tableSchema,
		colNameToIdx:   colNameToIdx,
		numPrimaryKeys: numPrimaryKeys,
	}
}
