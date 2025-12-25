package rawdocgrouper

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/yt/go/schema"
)

const updateTime = uint64(1677510100000000000)

type colParams struct {
	Name         string
	Value        interface{}
	IsPrimaryKey bool
}

func dummyItem(params []colParams) abstract.ChangeItem {
	return dummyItemWithKind(params, abstract.UpdateKind)
}

func dummyItemWithKind(params []colParams, kind abstract.Kind) abstract.ChangeItem {
	return dummyItemWithKindAndTable(params, kind, "table1")
}

var (
	col1Key = colParams{Name: "col1", IsPrimaryKey: true, Value: "1"}
	col2Key = colParams{Name: "col2", IsPrimaryKey: true, Value: "2"}
	col3Key = colParams{Name: "col3", IsPrimaryKey: true, Value: "3"}

	restValues = map[string]any{
		"innerKey1": "1",
		"innerKey2": 2,
		"innerKey3": true,
	}

	col1NotKey  = colParams{Name: "col1", IsPrimaryKey: false, Value: "1"}
	col1NotKey2 = colParams{Name: "col1", IsPrimaryKey: false, Value: 11}
	col2NotKey  = colParams{Name: "col2", IsPrimaryKey: false, Value: "2"}
	col2NotKey2 = colParams{Name: "col2", IsPrimaryKey: false, Value: "22"}
	col3NotKey  = colParams{Name: "col3", IsPrimaryKey: false, Value: "3"}
	col4NotKey  = colParams{Name: "col4", IsPrimaryKey: false, Value: 4}
	col4NotKey2 = colParams{Name: "col4", IsPrimaryKey: false, Value: 44}
	col5NotKey  = colParams{Name: "col5", IsPrimaryKey: false, Value: true}

	restCol = colParams{Name: restField, IsPrimaryKey: false, Value: restValues}
)

func dummyItemWithKindAndTable(params []colParams, kind abstract.Kind, table string) abstract.ChangeItem {
	chI := new(abstract.ChangeItem)
	var names []string
	var values []interface{}
	var keyNames []string
	var keyValues []interface{}
	var colschema []abstract.ColSchema
	for _, param := range params {
		_, ok := param.Value.(int)
		var dataType string
		if ok {
			dataType = schema.TypeInt32.String()
		} else {
			dataType = schema.TypeBytes.String()
		}
		colschema = append(colschema, abstract.ColSchema{
			PrimaryKey:   param.IsPrimaryKey,
			ColumnName:   param.Name,
			TableSchema:  "",
			TableName:    "",
			Path:         "",
			DataType:     dataType,
			FakeKey:      false,
			Required:     false,
			Expression:   "",
			OriginalType: "",
			Properties:   nil,
		})
		names = append(names, param.Name)
		values = append(values, param.Value)
		if param.IsPrimaryKey {
			keyNames = append(keyNames, param.Name)
			keyValues = append(keyValues, param.Value)
		}
	}

	if kind == abstract.DeleteKind {
		chI.OldKeys = abstract.OldKeysType{
			KeyNames:  keyNames,
			KeyTypes:  nil,
			KeyValues: keyValues,
		}
	} else {
		chI.ColumnNames = names
		chI.ColumnValues = values
	}

	chI.TableSchema = abstract.NewTableSchema(colschema)
	chI.CommitTime = updateTime
	chI.Kind = kind
	chI.Table = table
	chI.Schema = "public"
	return *chI
}

func compareColumns(t *testing.T, item abstract.ChangeItem, expectedCols []string) {

	sort.Strings(expectedCols)
	actualCols := make([]string, len(item.ColumnNames))
	copy(actualCols, item.ColumnNames)

	sort.Strings(actualCols)
	require.Equal(t, expectedCols, actualCols, "wrong columns!")
}

func transformAndCheckFields(t *testing.T, transformResult []abstract.ChangeItem, keys []string,
	fields []string, isEtlKey bool) abstract.ChangeItem {

	require.NotNil(t, transformResult[0].TableSchema, "Table schema should not be nil!")
	columns := transformResult[0].TableSchema.Columns()
	require.Equal(t, len(columns), len(keys)+len(fields), "Wrong number of columns in result!")

	//checking all pkeys and order
	for i := range keys {
		require.Equal(t, keys[i], columns[i].ColumnName, "%d column should be key %s", i, keys[i])
		require.True(t, columns[i].PrimaryKey, "Key column should be PK!")
	}

	columnsByName := make(map[string]abstract.ColSchema, len(columns))
	for _, col := range columns {
		columnsByName[col.ColumnName] = col
	}

	// checking non-keys
	for _, field := range fields {
		require.NotNil(t, columnsByName[field], "column should be present in data %s", field)
		require.False(t, columnsByName[field].PrimaryKey, "Non-key column %s should not be PK!", field)
	}

	// checking that all special fields are present
	for name := range rawDocFields {
		require.NotNil(t, columnsByName[name], "System column should be present in data %s", name)
	}
	require.Equal(t, isEtlKey, columnsByName[etlUpdatedField].PrimaryKey, "ETL column PK status is wrong!")
	return transformResult[0]
}
