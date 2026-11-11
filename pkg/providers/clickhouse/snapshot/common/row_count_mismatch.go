package ch_eo_common

import "fmt"

// RowCountMismatch distinguishes an incomplete private assembly from transport,
// schema and identity failures. A surplus is never eligible for an automatic retry.
type RowCountMismatch struct {
	hostName  string
	tableName string
	expected  uint64
	actual    uint64
}

func newRowCountMismatch(hostName, tableName string, expected, actual uint64) *RowCountMismatch {
	return &RowCountMismatch{hostName: hostName, tableName: tableName, expected: expected, actual: actual}
}

func (e *RowCountMismatch) Error() string {
	return fmt.Sprintf("snapshot row count mismatch on %s/%s: expected %d, found %d", e.hostName, e.tableName, e.expected, e.actual)
}

func (e *RowCountMismatch) HasMissingRows() bool {
	return e.actual < e.expected
}
