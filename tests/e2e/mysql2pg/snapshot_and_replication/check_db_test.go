package light

import (
	"database/sql"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/mysql"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement

	Source = *mysql.RecipeMysqlSource()

	dstPort, _ = strconv.Atoi(os.Getenv("PG_LOCAL_PORT"))
	Target     = provider_postgres.PgDestination{
		Hosts:     []string{"localhost"},
		ClusterID: os.Getenv("TARGET_CLUSTER_ID"),
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      dstPort,
		Cleanup:   model.Drop,
	}
)

func init() {
	_ = os.Setenv("YC", "1")                                                               // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "Mysql source", Port: Source.Port},
			network.LabeledPort{Label: "Pg target", Port: Target.Port},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Existence", Existence)
		t.Run("Snapshot", Snapshot)
		t.Run("Replication", Replication)
	})
}

func Existence(t *testing.T) {
	_, err := provider_mysql.NewStorage(Source.ToStorageParams())
	require.NoError(t, err)
	_, err = provider_postgres.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
}

func Snapshot(t *testing.T) {
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)
	_ = delivery.Activate(t, transfer)

	require.NoError(t, storagecomparison.WaitStoragesSynced(t, Source, Target, 30, storagecomparison.NewCompareStorageParams())) // 30 * 2 seconds should be enough
	//require.NoError(t, helpers.WaitDestinationEqualRowsCount(
	//	"source",
	//	"test",
	//	helpers.GetSampleableStorageByModel(t, Source),
	//	60*time.Second,
	//	2,
	//))
}

func Replication(t *testing.T) {
	cparams, err := provider_mysql.NewConnectionParams(Source.ToStorageParams())
	require.NoError(t, err)
	db, err := provider_mysql.Connect(cparams, nil)
	require.NoError(t, err)
	execCheck(t, db, "INSERT INTO test (id, val) VALUES (3, 'baz')")
	execCheck(t, db, "UPDATE test SET val = 'test' WHERE id = 1")
	execCheck(t, db, "DELETE FROM test WHERE id = 2")

	require.NoError(t, storagecomparison.WaitStoragesSynced(t, Source, Target, 30, storagecomparison.NewCompareStorageParams())) // 30 * 2 seconds should be enough
}

func execCheck(t *testing.T, db *sql.DB, query string) {
	res, err := db.Exec(query)
	require.NoError(t, err)
	rows, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

}
