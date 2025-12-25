package partitionedtable

import (
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	mysql "github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = *helpers.RecipeMysqlSource()
	Target       = *helpers.RecipeMysqlTarget(mysqlrecipe.WithPrefix("TARGET_"))
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, transferID
}

type TesttableRow struct {
	ID    int
	Value string
}

func TestPartitionedTable(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	sourceDB := connectToMysql(t, Source.ToStorageParams())
	defer sourceDB.Close()
	targetDB := connectToMysql(t, Target.ToStorageParams())
	defer targetDB.Close()

	checkTableIsEmpty(t, targetDB)

	testRow := TesttableRow{ID: 1, Value: "kek"}
	insertOneRow(t, sourceDB, testRow)

	require.NoError(t, helpers.WaitEqualRowsCountDifferentSchemas(t,
		Source.Database, Target.Database, "testtable",
		helpers.GetSampleableStorageByModel(t, Source),
		helpers.GetSampleableStorageByModel(t, Target),
		60*time.Second))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}

func connectToMysql(t *testing.T, storageParams *mysql.MysqlStorageParams) *sql.DB {
	connParams, err := mysql.NewConnectionParams(storageParams)
	require.NoError(t, err)

	db, err := mysql.Connect(connParams, nil)
	require.NoError(t, err)
	return db
}

func checkTableIsEmpty(t *testing.T, db *sql.DB) {
	var count int
	require.NoError(t, db.QueryRow(`select count(*) from testtable`).Scan(&count))
	require.Equal(t, 0, count)
}

func insertOneRow(t *testing.T, db *sql.DB, testRow TesttableRow) {
	_, err := db.Exec(`insert into testtable (id, value) values (?, ?)`, testRow.ID, testRow.Value)
	require.NoError(t, err)
}
