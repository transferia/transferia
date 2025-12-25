package snapshot

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("dump"), pgrecipe.WithDBTables("public.t2"))
	Target       = *pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	t.Run("Existence", Existence)
	t.Run("Snapshot", Snapshot)
}

func Existence(t *testing.T) {
	_, err := postgres.NewStorage(Source.ToStorageParams(nil))
	require.NoError(t, err)
	_, err = postgres.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
}

func Snapshot(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly)

	w := helpers.Activate(t, transfer)
	w.Close(t)

	dstStorage, err := postgres.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)

	exists, err := CheckTableExistence(context.Background(), dstStorage.Conn, "public", "t2")
	require.NoError(t, err)
	require.True(t, exists)

	exists, err = CheckTableExistence(context.Background(), dstStorage.Conn, "mysch", "t")
	require.NoError(t, err)
	require.False(t, exists)
}

// CheckTableExistence is a helper function for PostgreSQL to check existence of the given table
func CheckTableExistence(ctx context.Context, conn *pgxpool.Pool, tableSchema string, tableName string) (bool, error) {
	var result bool
	err := conn.QueryRow(context.Background(), `
		SELECT EXISTS
        (
            SELECT FROM information_schema.tables
			WHERE table_schema = $1 AND table_name = $2
        );
	`, tableSchema, tableName).Scan(&result)
	if err != nil {
		return false, xerrors.Errorf("check-table-existence query failed: %w", err)
	}
	return result, nil
}
