package replication

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("dump"))
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
	Target.PerTransactionPush = true
	t.Run("Main group", func(t *testing.T) {
		t.Run("Existence", Existence)
		t.Run("Snapshot", Snapshot)
		t.Run("Replication", Load)
	})
}

func Existence(t *testing.T) {
	_, err := postgres.NewStorage(Source.ToStorageParams(nil))
	require.NoError(t, err)
	_, err = postgres.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
}

func Snapshot(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	require.NoError(t, tasks.ActivateDelivery(context.Background(), nil, coordinator.NewFakeClient(), *transfer, helpers.EmptyRegistry()))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}

func Load(t *testing.T) {
	Target.CopyUpload = false
	Target.PerTransactionPush = true
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)

	Source.BatchSize = 10 * 1024 // to speedup repl
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	st, err := postgres.NewStorage(Source.ToStorageParams(nil))
	require.NoError(t, err)
	defer st.Close()
	_, err = st.Conn.Exec(context.Background(), "delete from __test where id > 10")
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "__test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 180*time.Second))

	//-----------------------------------------------------------------------------------------------------------------

	conn := st.Conn

	_, err = conn.Exec(context.Background(), "INSERT INTO trash (title) VALUES ('xyz');")
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), "INSERT INTO pkey_only (key1, key2) VALUES ('bar', 'baz');")
	require.NoError(t, err)
	// Real update changing value
	_, err = conn.Exec(context.Background(), "UPDATE pkey_only SET key2 = 'barbar' WHERE key1 = 'foo';")
	require.NoError(t, err)

	helpers.CheckRowsCount(t, Source, "public", "trash", 1)
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "trash", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 180*time.Second))

	// "Fake" update, does not change anything in DB but is present in WAL
	_, err = conn.Exec(context.Background(), "UPDATE pkey_only SET key2 = 'baz' WHERE key1 = 'bar';")
	require.NoError(t, err)

	_, err = conn.Exec(context.Background(), "INSERT INTO __test (id, title) VALUES (11, 'abc'), (12, 'def');")
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "__test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 180*time.Second))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
