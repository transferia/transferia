package insufficientprivileges

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	cpclient "github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
)

func init() {
	_ = pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source")) // to init test container
}

func TestSnapshotWithEmptyTableListFails(t *testing.T) {
	var emptyIncludeTables, emptyExcludeTables []string
	transfer := helpers.MakeTransfer(helpers.TransferID, newSource(emptyIncludeTables, emptyExcludeTables), newTarget(), abstract.TransferTypeSnapshotOnly)
	err := tasks.ActivateDelivery(context.Background(), nil, cpclient.NewFakeClient(), *transfer, helpers.EmptyRegistry())
	require.Error(t, err)
	require.Contains(t, err.Error(), "permission denied")
}

func TestSnapshotWithAccessToPublicTableWorks(t *testing.T) {
	includeTables := []string{"public.promiscuous"}
	var emptyExcludeTables []string
	for _, transferType := range []abstract.TransferType{abstract.TransferTypeSnapshotOnly, abstract.TransferTypeSnapshotAndIncrement} {
		transfer := helpers.MakeTransfer(helpers.TransferID, newSource(includeTables, emptyExcludeTables), newTarget(), transferType)
		err := tasks.ActivateDelivery(context.Background(), nil, cpclient.NewFakeClient(), *transfer, helpers.EmptyRegistry())
		require.NoError(t, err)
	}
}

func TestSnapshotWithInsufficientPermissionsToSpecificTableFails(t *testing.T) {
	includeTables := []string{"public.promiscuous", "public.secret"}
	var emptyExcludeTables []string
	for _, transferType := range []abstract.TransferType{abstract.TransferTypeSnapshotOnly, abstract.TransferTypeSnapshotAndIncrement} {
		transfer := helpers.MakeTransfer(helpers.TransferID, newSource(includeTables, emptyExcludeTables), newTarget(), transferType)
		err := tasks.ActivateDelivery(context.Background(), nil, cpclient.NewFakeClient(), *transfer, helpers.EmptyRegistry())
		require.Error(t, err)
		require.Contains(t, err.Error(), "Tables not found")
		require.Contains(t, err.Error(), "public.secret")
	}
}

func TestAddTableInSource(t *testing.T) {
	emptyIncludeTables := []string{}
	excludeTables := []string{"public.secret"} // Activation will fail with error if we don't exclude this table

	// Activate
	transfer := helpers.MakeTransfer(helpers.TransferID, newSource(emptyIncludeTables, excludeTables), newTarget(), abstract.TransferTypeSnapshotAndIncrement)
	err := tasks.ActivateDelivery(context.Background(), nil, cpclient.NewFakeClient(), *transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	// Replication, first try
	transfer.Dst.(*postgres.PgDestination).CopyUpload = false // :(
	wrkr := local.NewLocalWorker(cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)
	defer wrkr.Stop()
	runChannel := make(chan error)
	go func() { runChannel <- wrkr.Run() }()

	// Wait until replication has started and transfers one row
	insertOneRow(t, helpers.GetIntFromEnv("SOURCE_PG_LOCAL_PORT"), "public.promiscuous", tableRow{100, "100"})
	for rowCount(t, helpers.GetIntFromEnv("TARGET_PG_LOCAL_PORT"), "public.promiscuous") < 4 {
		time.Sleep(time.Second)
	}
	require.Equal(t, 4, rowCount(t, helpers.GetIntFromEnv("TARGET_PG_LOCAL_PORT"), "public.promiscuous"))

	// Add table with one row to the source database and create an empty one in the target
	createTable(t, helpers.GetIntFromEnv("SOURCE_PG_LOCAL_PORT"), "public.secret2")
	createTable(t, helpers.GetIntFromEnv("TARGET_PG_LOCAL_PORT"), "public.secret2")
	insertOneRow(t, helpers.GetIntFromEnv("SOURCE_PG_LOCAL_PORT"), "public.secret2", tableRow{100, "100"})

	// Expect replication to fail now
	err = <-runChannel
	require.Error(t, err)
	require.False(t, abstract.IsFatal(err))
	require.Equal(t, 0, rowCount(t, helpers.GetIntFromEnv("TARGET_PG_LOCAL_PORT"), "public.secret2"))

	// Replication, second try
	wrkr = local.NewLocalWorker(cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)
	defer wrkr.Stop()

	err = wrkr.Run()
	require.Error(t, err)
	require.False(t, abstract.IsFatal(err))
	require.Equal(t, 0, rowCount(t, helpers.GetIntFromEnv("TARGET_PG_LOCAL_PORT"), "public.secret2"))

	// Give access to the source table secret2 to loser and check that replication works after that
	grantPrivileges(t, helpers.GetIntFromEnv("SOURCE_PG_LOCAL_PORT"), "public.secret2", "loser")
	wrkr = local.NewLocalWorker(cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)
	defer wrkr.Stop()
	wrkr.Start() // Use asynchronous Start() instead of synchronous Run() to avoid blocking
	for rowCount(t, helpers.GetIntFromEnv("TARGET_PG_LOCAL_PORT"), "public.secret2") == 0 {
		time.Sleep(time.Second)
	}
	require.Equal(t, 1, rowCount(t, helpers.GetIntFromEnv("TARGET_PG_LOCAL_PORT"), "public.secret2"))
}
