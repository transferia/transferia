package replication

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/postgres"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/transfer"
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
		pgrecipe.WithEdit(func(pg *provider_postgres.PgSource) { pg.UseFakePrimaryKey = true }),
	)
	SrcOnlyPartitionedTable = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir(yatestx.ProjectSource("dump")),
		pgrecipe.WithDBTables(Tables...),
		pgrecipe.WithEdit(func(pg *provider_postgres.PgSource) { pg.UseFakePrimaryKey = true }),
	)
	Dst = *pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
)

func init() {
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &SrcWholeDB, &Dst, transferType)
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &SrcOnlyPartitionedTable, &Dst, transferType)
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: SrcOnlyPartitionedTable.Port},
			network.LabeledPort{Label: "PG target", Port: Dst.Port},
		))
	}()

	t.Run("Existence and source check", Existence)
	t.Run("TransferParentWithAllChildren", TransferParentWithAllChildren)
	t.Run("TransferParentWithAllChildrenCollapseIgnoredForHomo", TransferParentWithAllChildrenCollapseIgnoredForHomo)
	t.Run("TransferParentAndDataTables", TransferParentAndDataTables)
}

func Existence(t *testing.T) {
	configs := []*provider_postgres.PgStorageParams{SrcOnlyPartitionedTable.ToStorageParams(nil), Dst.ToStorageParams()}
	for _, config := range configs {
		storage, err := provider_postgres.NewStorage(config)
		require.NoError(t, err)
		storage.Close()
	}
	storagecomparison.CheckRowsCount(t, &SrcOnlyPartitionedTable, tableSchema, partitionedTable, totalPartitionedRows)
	storagecomparison.CheckRowsCount(t, &SrcOnlyPartitionedTable, tableSchema, partName2, rowsPartitionY2006m02)
	storagecomparison.CheckRowsCount(t, &SrcOnlyPartitionedTable, tableSchema, partName3, rowsPartitionY2006m03)
	storagecomparison.CheckRowsCount(t, &SrcOnlyPartitionedTable, tableSchema, partName4, rowsPartitionY2006m04)
	storagecomparison.CheckRowsCount(t, &SrcOnlyPartitionedTable, tableSchema, partName5, rowsPartitionY2006m05)
	storagecomparison.CheckRowsCount(t, &SrcOnlyPartitionedTable, tableSchema, notPartitionedTable, notPartitionedRowsInSrc)
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
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &SrcOnlyPartitionedTable, &Dst, transferType)
	src := transfer.Src.(*provider_postgres.PgSource)
	require.True(t, src.IsHomo, "this test validates homo pg->pg behavior")
	src.CollapseInheritTables = collapseInheritTables
	resetTargetPartitionedTables(t, transfer.Dst)
	transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{tableSchema + "." + partitionedTable}}

	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	storagecomparison.CheckRowsCount(t, transfer.Dst, tableSchema, partitionedTable, totalPartitionedRows)
	storagecomparison.CheckRowsCount(t, transfer.Dst, tableSchema, partName2, rowsPartitionY2006m02)
	storagecomparison.CheckRowsCount(t, transfer.Dst, tableSchema, partName3, rowsPartitionY2006m03)
	storagecomparison.CheckRowsCount(t, transfer.Dst, tableSchema, partName4, rowsPartitionY2006m04)
	storagecomparison.CheckRowsCount(t, transfer.Dst, tableSchema, partName5, rowsPartitionY2006m05)

	storage, ok := storagecomparison.GetSampleableStorageByModel(t, transfer.Dst).(*provider_postgres.Storage)
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
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &SrcWholeDB, &Dst, transferType)
	src := transfer.Src.(*provider_postgres.PgSource)
	src.CollapseInheritTables = false
	resetTargetPartitionedTables(t, transfer.Dst)
	transfer.DataObjects = &model.DataObjects{
		IncludeObjects: []string{
			tableSchema + "." + partitionedTable,
			tableSchema + "." + partName2,
		},
	}

	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	storagecomparison.CheckRowsCount(t, transfer.Dst, tableSchema, partitionedTable, rowsPartitionY2006m02)
	storagecomparison.CheckRowsCount(t, transfer.Dst, tableSchema, partName2, rowsPartitionY2006m02)
	storagecomparison.CheckRowsCount(t, transfer.Dst, tableSchema, partName3, 0)
	storagecomparison.CheckRowsCount(t, transfer.Dst, tableSchema, partName4, 0)
	storagecomparison.CheckRowsCount(t, transfer.Dst, tableSchema, partName5, 0)
}

func resetTargetPartitionedTables(t *testing.T, dst model.Destination) {
	storage, ok := storagecomparison.GetSampleableStorageByModel(t, dst).(*provider_postgres.Storage)
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
