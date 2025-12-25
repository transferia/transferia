package abstract

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSortParts(t *testing.T) {
	part1 := &OperationTablePart{
		OperationID:   "1",
		Schema:        "schema1",
		Name:          "table1",
		Offset:        1,
		CompletedRows: 3,
	}
	part2 := &OperationTablePart{
		OperationID:   "2",
		Schema:        "schema2",
		Name:          "table2",
		Offset:        2,
		CompletedRows: 15,
	}
	part3 := &OperationTablePart{
		OperationID:   "1",
		Schema:        "schema1",
		Name:          "table1",
		Offset:        1,
		CompletedRows: 3,
		Completed:     true,
	}
	part4 := &OperationTablePart{
		OperationID:   "1",
		Schema:        "schema1",
		Name:          "table1",
		Offset:        1,
		CompletedRows: 2,
	}
	part5 := &OperationTablePart{
		OperationID:   "1",
		Schema:        "schema1",
		Name:          "table1",
		Offset:        3,
		CompletedRows: 1,
	}

	initialParts := []*OperationTablePart{part1, part2, part3, part4, part5, part1}

	deduplicatedParts := SortAndDeduplicateTableParts(initialParts)
	require.Len(t, deduplicatedParts, 3)
	require.Equal(t, deduplicatedParts, []*OperationTablePart{part3, part5, part2})

	SortTableParts(initialParts)
	require.Len(t, initialParts, 6)
	require.Equal(t, initialParts[0].Key(), "OperationID: 1, Name: 'table1', Schema: 'schema1', Part: 0, Offset: 1, Filter: ''")
	require.Equal(t, initialParts[1].Key(), "OperationID: 1, Name: 'table1', Schema: 'schema1', Part: 0, Offset: 1, Filter: ''")
	require.Equal(t, initialParts[2].Key(), "OperationID: 1, Name: 'table1', Schema: 'schema1', Part: 0, Offset: 1, Filter: ''")
	require.Equal(t, initialParts[3].Key(), "OperationID: 1, Name: 'table1', Schema: 'schema1', Part: 0, Offset: 1, Filter: ''")
	require.Equal(t, initialParts[4].Key(), "OperationID: 1, Name: 'table1', Schema: 'schema1', Part: 0, Offset: 3, Filter: ''")
	require.Equal(t, initialParts[5].Key(), "OperationID: 2, Name: 'table2', Schema: 'schema2', Part: 0, Offset: 2, Filter: ''")
}
