package schemamigration

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump/pg"))
	Target       = *pgrecipe.RecipeTarget()
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Target.Cleanup = model.DisabledCleanup
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType)
}

func TestAddedColumnIsMigratedOnSnapshot(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()
	require.False(t, Target.IsSchemaMigrationDisabled)
	require.False(t, Target.MaintainTables)

	srcConn, err := provider_postgres.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()
	dstConn, err := provider_postgres.MakeConnPoolFromDst(&Target, logger.Log)
	require.NoError(t, err)
	defer dstConn.Close()

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)

	helpers.Activate(t, transfer)
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
	require.False(t, targetHasColumn(t, dstConn, "is_agent"))

	_, err = srcConn.Exec(context.Background(), `ALTER TABLE __test ADD COLUMN is_agent boolean NOT NULL DEFAULT false`)
	require.NoError(t, err)
	_, err = srcConn.Exec(context.Background(), `INSERT INTO __test (id, val, is_agent) VALUES (3, 'c', true)`)
	require.NoError(t, err)

	helpers.Activate(t, transfer)

	require.True(t, targetHasColumn(t, dstConn, "is_agent"))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}

func targetHasColumn(t *testing.T, conn *pgxpool.Pool, column string) bool {
	var exists bool
	require.NoError(t, conn.QueryRow(context.Background(), `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = '__test' AND column_name = $1
		)`, column).Scan(&exists))
	return exists
}
