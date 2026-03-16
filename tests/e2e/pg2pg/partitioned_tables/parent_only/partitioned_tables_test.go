package replication

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
	"github.com/transferia/transferia/tests/helpers/yatestx"
)

const (
	transferType = abstract.TransferTypeSnapshotAndIncrement

	tableSchema         = "public"
	partitionedTable    = "partitioned_table"
	notPartitionedTable = "not_partitioned"

	partName2 = "partitioned_table_y2006m02"
	partName3 = "partitioned_table_y2006m03"
	partName4 = "partitioned_table_y2006m04"
	partName5 = "partitioned_table_y2006m05"

	totalPartitionedRows    = 10
	rowsPartitionY2006m02   = 2
	rowsPartitionY2006m03   = 3
	rowsPartitionY2006m04   = 3
	rowsPartitionY2006m05   = 2
	notPartitionedRowsInSrc = 1
)

var (
	Tables     = []string{tableSchema + "." + partitionedTable}
	SrcWholeDB = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir(yatestx.ProjectSource("dump")),
		pgrecipe.WithEdit(func(pg *postgres.PgSource) { pg.UseFakePrimaryKey = true }),
	)
	SrcOnlyPartitionedTable = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir(yatestx.ProjectSource("dump")),
		pgrecipe.WithDBTables(Tables...),
		pgrecipe.WithEdit(func(pg *postgres.PgSource) { pg.UseFakePrimaryKey = true }),
	)
	Dst = *pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
)

func init() {
	helpers.InitSrcDst(helpers.TransferID, &SrcWholeDB, &Dst, transferType)
	helpers.InitSrcDst(helpers.TransferID, &SrcOnlyPartitionedTable, &Dst, transferType)
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: SrcOnlyPartitionedTable.Port},
			helpers.LabeledPort{Label: "PG target", Port: Dst.Port},
		))
	}()

	t.Run("Existence and source check", Existence)
	t.Run("TransferParentWithAllChildren", TransferParentWithAllChildren)
	t.Run("TransferParentWithAllChildrenCollapseIgnoredForHomo", TransferParentWithAllChildrenCollapseIgnoredForHomo)
	t.Run("TransferParentAndDataTables", TransferParentAndDataTables)
}

func Existence(t *testing.T) {
	configs := []*postgres.PgStorageParams{SrcOnlyPartitionedTable.ToStorageParams(nil), Dst.ToStorageParams()}
	for _, config := range configs {
		storage, err := postgres.NewStorage(config)
		require.NoError(t, err)
		storage.Close()
	}
	helpers.CheckRowsCount(t, &SrcOnlyPartitionedTable, tableSchema, partitionedTable, totalPartitionedRows)
	helpers.CheckRowsCount(t, &SrcOnlyPartitionedTable, tableSchema, partName2, rowsPartitionY2006m02)
	helpers.CheckRowsCount(t, &SrcOnlyPartitionedTable, tableSchema, partName3, rowsPartitionY2006m03)
	helpers.CheckRowsCount(t, &SrcOnlyPartitionedTable, tableSchema, partName4, rowsPartitionY2006m04)
	helpers.CheckRowsCount(t, &SrcOnlyPartitionedTable, tableSchema, partName5, rowsPartitionY2006m05)
	helpers.CheckRowsCount(t, &SrcOnlyPartitionedTable, tableSchema, notPartitionedTable, notPartitionedRowsInSrc)
}

// TransferParentWithAllChildren checks that selecting only parent transfers parent and all data tables.
func TransferParentWithAllChildren(t *testing.T) {
	transferParentWithAllChildren(t, false)
}

// TransferParentWithAllChildrenCollapseIgnoredForHomo checks that for homo transfer CollapseInheritTables does not change parent-only behavior.
func TransferParentWithAllChildrenCollapseIgnoredForHomo(t *testing.T) {
	transferParentWithAllChildren(t, true)
}

func transferParentWithAllChildren(t *testing.T, collapseInheritTables bool) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &SrcOnlyPartitionedTable, &Dst, transferType)
	src := transfer.Src.(*postgres.PgSource)
	require.True(t, src.IsHomo, "this test validates homo pg->pg behavior")
	src.CollapseInheritTables = collapseInheritTables
	resetTargetPartitionedTables(t, transfer.Dst)
	transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{tableSchema + "." + partitionedTable}}

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	helpers.CheckRowsCount(t, transfer.Dst, tableSchema, partitionedTable, totalPartitionedRows)
	helpers.CheckRowsCount(t, transfer.Dst, tableSchema, partName2, rowsPartitionY2006m02)
	helpers.CheckRowsCount(t, transfer.Dst, tableSchema, partName3, rowsPartitionY2006m03)
	helpers.CheckRowsCount(t, transfer.Dst, tableSchema, partName4, rowsPartitionY2006m04)
	helpers.CheckRowsCount(t, transfer.Dst, tableSchema, partName5, rowsPartitionY2006m05)

	storage, ok := helpers.GetSampleableStorageByModel(t, transfer.Dst).(*postgres.Storage)
	require.True(t, ok)

	var exists bool
	require.NoError(t, storage.Conn.QueryRow(context.Background(), `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = $1 AND table_name = $2
		)`, tableSchema, notPartitionedTable).Scan(&exists))
	require.False(t, exists)
}

// TransferParentAndDataTables checks that selecting parent together with some leaf partitions transfers only selected parts.
func TransferParentAndDataTables(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &SrcWholeDB, &Dst, transferType)
	src := transfer.Src.(*postgres.PgSource)
	src.CollapseInheritTables = false
	resetTargetPartitionedTables(t, transfer.Dst)
	transfer.DataObjects = &model.DataObjects{
		IncludeObjects: []string{
			tableSchema + "." + partitionedTable,
			tableSchema + "." + partName2,
		},
	}

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	helpers.CheckRowsCount(t, transfer.Dst, tableSchema, partitionedTable, rowsPartitionY2006m02)
	helpers.CheckRowsCount(t, transfer.Dst, tableSchema, partName2, rowsPartitionY2006m02)
	helpers.CheckRowsCount(t, transfer.Dst, tableSchema, partName3, 0)
	helpers.CheckRowsCount(t, transfer.Dst, tableSchema, partName4, 0)
	helpers.CheckRowsCount(t, transfer.Dst, tableSchema, partName5, 0)
}

func resetTargetPartitionedTables(t *testing.T, dst model.Destination) {
	storage, ok := helpers.GetSampleableStorageByModel(t, dst).(*postgres.Storage)
	require.True(t, ok)
	_, err := storage.Conn.Exec(context.Background(), `
		DROP TABLE IF EXISTS public.partitioned_table CASCADE;
		DROP TABLE IF EXISTS public.partitioned_table_y2006m02 CASCADE;
		DROP TABLE IF EXISTS public.partitioned_table_y2006m03 CASCADE;
		DROP TABLE IF EXISTS public.partitioned_table_y2006m04 CASCADE;
		DROP TABLE IF EXISTS public.partitioned_table_y2006m05 CASCADE;
		DROP TABLE IF EXISTS public.not_partitioned CASCADE;
	`)
	require.NoError(t, err)
}
