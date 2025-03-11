package helpers

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/library/go/test/canon"
	"github.com/transferria/transferria/pkg/abstract"
)

func GenerateCanonCheckerValues(t *testing.T, changeItem *abstract.ChangeItem) {
	colNameToOriginalType := make(map[string]string)
	for _, el := range changeItem.TableSchema.Columns() {
		colNameToOriginalType[el.ColumnName] = el.OriginalType
	}
	fmt.Println(`func checkCanonizedTypesOverValues(t *testing.T, changeItem *abstract.ChangeItem) {`)
	fmt.Println(`    m := make(map[string]interface{})`)
	fmt.Println(`    for i := range changeItem.ColumnNames {`)
	fmt.Println(`        m[changeItem.ColumnNames[i]] = changeItem.ColumnValues[i]`)
	fmt.Println(`    }`)
	for i := range changeItem.ColumnNames {
		currColName := changeItem.ColumnNames[i]
		currColVal := changeItem.ColumnValues[i]
		currOriginalType, ok := colNameToOriginalType[currColName]
		require.True(t, ok)
		fmt.Printf("    require.Equal(t, \"%s\", fmt.Sprintf(\"%%T\", m[\"%s\"]), `%s:%s`)\n", fmt.Sprintf("%T", currColVal), currColName, currColName, currOriginalType)
	}
	fmt.Println(`}`)
}

func CanonizeTableChangeItems(t *testing.T, storage abstract.Storage, table abstract.TableDescription) {
	result := make([]abstract.ChangeItem, 0)
	err := storage.LoadTable(context.Background(), table, func(input []abstract.ChangeItem) error {
		result = append(result, input...)
		return nil
	})
	require.NoError(t, err)
	for i := range result {
		result[i].CommitTime = 0
	}
	canon.SaveJSON(t, result)
}
