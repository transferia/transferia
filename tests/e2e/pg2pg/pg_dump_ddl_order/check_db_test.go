package pg_dump_ddl_order

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
	"github.com/transferia/transferia/tests/helpers/yatestx"
)

var (
	transferType = abstract.TransferTypeSnapshotOnly
	source       = *pgrecipe.RecipeSource(pgrecipe.WithInitDir(yatestx.ProjectSource("dump")), pgrecipe.WithPrefix(""))
	target       = *pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
)

func init() {
	_ = os.Setenv("YC", "1")
	helpers.InitSrcDst(helpers.TransferID, &source, &target, transferType)
}

func TestDDLOrderPreSteps(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: source.Port},
			helpers.LabeledPort{Label: "PG target", Port: target.Port},
		))
	}()

	t.Run("Existence", func(t *testing.T) {
		_, err := postgres.NewStorage(source.ToStorageParams(nil))
		require.NoError(t, err)
		_, err = postgres.NewStorage(target.ToStorageParams())
		require.NoError(t, err)
	})

	t.Run("PreStepsApplyTableBeforeFunction", func(t *testing.T) {
		src := source
		src.DBTables = []string{"public.dependant_table"}
		preSteps := *source.PreSteps
		preSteps.Function = true
		src.PreSteps = &preSteps
		src.WithDefaults()

		transfer := helpers.MakeTransfer(helpers.TransferID, &src, &target, transferType)

		items, err := postgres.ExtractPgDumpSchema(transfer)
		require.NoError(t, err)
		require.NotEmpty(t, items)

		require.NoError(t, postgres.ApplyPgDumpPreSteps(items, transfer, &model.TransferOperation{}, helpers.EmptyRegistry()))

		dstStorage, err := postgres.NewStorage(target.ToStorageParams())
		require.NoError(t, err)
		defer dstStorage.Close()
		var tableCnt, funcCnt int
		require.NoError(t, dstStorage.Conn.QueryRow(t.Context(),
			`SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public' AND tablename = 'dependant_table'`).Scan(&tableCnt))
		require.NoError(t, dstStorage.Conn.QueryRow(t.Context(),
			`SELECT COUNT(*) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE n.nspname = 'public' AND p.proname = 'func_using_table'`).Scan(&funcCnt))
		require.Equal(t, 1, tableCnt)
		require.Equal(t, 1, funcCnt)
	})
}
