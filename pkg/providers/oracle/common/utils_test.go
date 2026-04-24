package common

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetTablesCondition(t *testing.T) {
	schemaColumnName := "schema"
	tableColumnName := "table"
	tableIDs := make([]*TableID, 0)

	for i := 0; i < 1001; i++ {
		tableID := TableID{
			tableName:  "myTable",
			schemaName: "mySchema",
		}
		tableIDs = append(tableIDs, &tableID)
	}

	expectedSuffix := "or '\"'||schema||'\".\"'||table||'\"' in ('\"mySchema\".\"myTable\"'))"
	result, err := GetTablesCondition(schemaColumnName, tableColumnName, tableIDs, true)
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(result, expectedSuffix))

	expectedSuffix = "and '\"'||schema||'\".\"'||table||'\"' not in ('\"mySchema\".\"myTable\"'))"
	result, err = GetTablesCondition(schemaColumnName, tableColumnName, tableIDs, false)
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(result, expectedSuffix))
}
