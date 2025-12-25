package test

import (
	"os"
	"strings"
	"testing"

	"cuelang.org/go/pkg/time"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/tests/helpers"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

func TestReplicaIdentityFullInsert(t *testing.T) {
	perTransactionPush := false
	testReplicationWorks(t, "testslot1", "__replica_id_full_1", perTransactionPush, untilStoragesEqual)
}

func TestReplicaIdentityFullDelete(t *testing.T) {
	perTransactionPush := false
	testReplicationWorks(t, "testslot2", "__replica_id_full_2", perTransactionPush, untilStoragesEqual)
}

func TestReplicaIdentityFullUpdate(t *testing.T) {
	perTransactionPush := false
	testReplicationWorks(t, "testslot3", "__replica_id_full_3", perTransactionPush, untilStoragesEqual)
}

func TestReplicaIdentityFullInsertRetry(t *testing.T) {
	perTransactionPush := false
	// We use two replication slots here to emulate replication retry.
	// First we replicate one insert from testslot4_1. Both source and target
	// will have 4 rows after that.
	testReplicationWorks(t, "testslot4_1", "__replica_id_full_4", perTransactionPush, untilStoragesEqual)
	// Then we replicate the same insert with the same LSN from the other slot.
	// If the table in the destination had primary key constraint, nothing
	// would be replicated. But instead we will have a duplicate row in the
	// destination.
	testReplicationWorks(t, "testslot4_2", "__replica_id_full_4", perTransactionPush, untilDestinationRowCountEquals(5))
}

func TestReplicaIdentityFullInsertRetryWithPerTransactionPush(t *testing.T) {
	perTransactionPush := true
	// Same as above, use two slots to emulate replication retry
	testReplicationWorks(t, "testslot5_1", "__replica_id_full_5", perTransactionPush, untilStoragesEqual)
	// Replicate for a while and compare the storages. We wait 30 seconds to
	// ensure that with perTransactionPush = true no duplicate rows are
	// inserted into the destination during that time.
	testReplicationWorks(t, "testslot5_2", "__replica_id_full_5", perTransactionPush, untilTimeElapsesAndStoragesEqual(30*time.Second, 4))
}

func TestReplicaIdentityNotFullFails(t *testing.T) {
	source := *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"), pgrecipe.WithDBTables("public.__replica_id_not_full"))
	source.SlotID = "testslot6"
	target := *pgrecipe.RecipeTarget(pgrecipe.WithInitDir("init_target"))

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: source.Port},
			helpers.LabeledPort{Label: "PG target", Port: target.Port},
		))
	}()

	TransferType := abstract.TransferTypeIncrementOnly
	helpers.InitSrcDst(helpers.TransferID, &source, &target, TransferType)

	replicationWorker := local.NewLocalWorker(
		coordinator.NewFakeClient(),
		helpers.MakeTransfer(helpers.TransferID, &source, &target, TransferType),
		helpers.EmptyRegistry(),
		logger.Log,
	)
	err := replicationWorker.Run()
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "no key columns found")
	err = replicationWorker.Stop()
	require.NoError(t, err)
}
