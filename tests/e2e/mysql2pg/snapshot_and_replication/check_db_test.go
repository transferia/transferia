package light

import (
	"database/sql"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement

	Source = *helpers.RecipeMysqlSource()

	dstPort, _ = strconv.Atoi(os.Getenv("PG_LOCAL_PORT"))
	Target     = postgres.PgDestination{
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
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: Source.Port},
			helpers.LabeledPort{Label: "Pg target", Port: Target.Port},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Existence", Existence)
		t.Run("Snapshot", Snapshot)
		t.Run("Replication", Replication)
	})
}

func Existence(t *testing.T) {
	_, err := mysql.NewStorage(Source.ToStorageParams())
	require.NoError(t, err)
	_, err = postgres.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
}

func Snapshot(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	_ = helpers.Activate(t, transfer)

	require.NoError(t, helpers.WaitStoragesSynced(t, Source, Target, 30, helpers.NewCompareStorageParams())) // 30 * 2 seconds should be enough
	//require.NoError(t, helpers.WaitDestinationEqualRowsCount(
	//	"source",
	//	"test",
	//	helpers.GetSampleableStorageByModel(t, Source),
	//	60*time.Second,
	//	2,
	//))
}

func Replication(t *testing.T) {
	cparams, err := mysql.NewConnectionParams(Source.ToStorageParams())
	require.NoError(t, err)
	db, err := mysql.Connect(cparams, nil)
	require.NoError(t, err)
	execCheck(t, db, "INSERT INTO test (id, val) VALUES (3, 'baz')")
	execCheck(t, db, "UPDATE test SET val = 'test' WHERE id = 1")
	execCheck(t, db, "DELETE FROM test WHERE id = 2")

	require.NoError(t, helpers.WaitStoragesSynced(t, Source, Target, 30, helpers.NewCompareStorageParams())) // 30 * 2 seconds should be enough
}

func execCheck(t *testing.T, db *sql.DB, query string) {
	res, err := db.Exec(query)
	require.NoError(t, err)
	rows, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

}
