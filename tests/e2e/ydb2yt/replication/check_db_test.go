package main

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/tests/helpers/delivery"
	_ "github.com/transferia/transferia/tests/helpers/registration/ydb"
	_ "github.com/transferia/transferia/tests/helpers/registration/yt"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	"github.com/transferia/transferia/tests/helpers/ydb/testdata"
)

func TestSnapshotAndReplication(t *testing.T) {
	currTableName := "test_table"

	source := &provider_ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           testenv.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           testenv.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             []string{currTableName},
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
		ChangeFeedMode:     provider_ydb.ChangeFeedModeUpdates,
	}
	target := provider_yt.NewYtDestinationV1(provider_yt.YtDestination{
		Path:                     "//home/cdc/test/pg2yt_e2e",
		Cluster:                  os.Getenv("YT_PROXY"),
		CellBundle:               "default",
		PrimaryMedium:            "default",
		UseStaticTableOnSnapshot: true, // TM-4444
	})
	transferType := abstract.TransferTypeSnapshotAndIncrement
	transferhelpers.InitSrcDst(transferhelpers.TransferID, source, target, transferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable

	//---

	Target := &provider_ydb.YdbDestination{
		Database: source.Database,
		Token:    source.Token,
		Instance: source.Instance,
	}
	Target.WithDefaults()
	srcSink, err := provider_ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	// insert one rec - for snapshot uploading

	currChangeItem := testdata.YDBStmtInsert(t, currTableName, 1)
	require.NoError(t, srcSink.Push([]abstract.ChangeItem{*currChangeItem}))

	// start snapshot & replication

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, source, target, transferType)
	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	storagecomparison.CheckRowsCount(t, target, "", currTableName, 1)

	// insert two more records - it's three of them now

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{
		*testdata.YDBStmtInsert(t, currTableName, 2),
		*testdata.YDBStmtInsert(t, currTableName, 3),
	}))

	// update 2nd rec

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{
		*testdata.YDBStmtUpdate(t, currTableName, 2, 666),
	}))

	// update 3rd rec by TOAST

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{
		*testdata.YDBStmtUpdateTOAST(t, currTableName, 3, 777),
	}))

	// delete 1st rec

	require.NoError(t, srcSink.Push([]abstract.ChangeItem{
		*testdata.YDBStmtDelete(t, currTableName, 1),
	}))

	// check

	require.NoError(t, storage.WaitDestinationEqualRowsCount("", currTableName, storagecomparison.GetSampleableStorageByModel(t, target), 60*time.Second, 2))
}
