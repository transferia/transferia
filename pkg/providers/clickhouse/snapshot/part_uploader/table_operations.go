package part_uploader

import (
	"context"

	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
)

// TableOperations prepares and renames temporary tables using the database's
// DDL rules. It is bound to the source table's publication record.
type TableOperations interface {
	WaitForStagingDDL(context.Context) error
	PrepareLoadingTables(context.Context, string) ([]ch_eo_common.OwnedTable, error)
	RenamePartTable(context.Context, *ch_eo_common.OwnedTable, string) error
	PartTableIdentityHostName(string) string
}
