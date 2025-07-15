package geometry_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	mysql_client "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	TransferType = abstract.TransferTypeIncrementOnly
	Source       = *helpers.RecipeMysqlSource()
	Target       = *helpers.RecipeMysqlTarget(mysqlrecipe.WithPrefix("TARGET_"))
)

func init() {
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: Source.Port},
			helpers.LabeledPort{Label: "Mysql target", Port: Target.Port},
		))
	}()

	t.Run("Main group", func(t *testing.T) {
		t.Run("Existence", Existence)
		t.Run("Snapshot", Snapshot)
		t.Run("Replication", Replication)
	})
}

func Existence(t *testing.T) {
	_, err := mysql.NewStorage(Source.ToStorageParams())
	require.NoError(t, err)
	_, err = mysql.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
}

func Snapshot(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly)

	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	err = snapshotLoader.UploadTables(context.TODO(), tables.ConvertToTableDescriptions(), true)
	require.NoError(t, err)

	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}

func Replication(t *testing.T) {
	sourceAsDestination := mysql.MysqlDestination{
		Host:     Source.Host,
		User:     Source.User,
		Password: Source.Password,
		Database: Source.Database,
		Port:     Source.Port,
	}
	sourceAsDestination.WithDefaults()
	_, err := mysql.NewSinker(logger.Log, &sourceAsDestination, helpers.EmptyRegistry())
	require.NoError(t, err)

	transfer := &model.Transfer{
		ID:  "test-id",
		Src: &Source,
		Dst: &Target,
	}

	fakeClient := coordinator.NewStatefulFakeClient()
	err = mysql.SyncBinlogPosition(&Source, transfer.ID, fakeClient)
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(fakeClient, transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	cfg := mysql_client.NewConfig()
	cfg.Addr = fmt.Sprintf("%v:%v", Source.Host, Source.Port)
	cfg.User = Source.User
	cfg.Passwd = string(Source.Password)
	cfg.DBName = Source.Database
	cfg.Net = "tcp"

	mysqlConnector, err := mysql_client.NewConnector(cfg)
	require.NoError(t, err)
	db := sql.OpenDB(mysqlConnector)

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	requests := []string{
		`insert into geo_test(g, p, l, poly, mp, ml, mpoly, gs) values
		(
			ST_GeomFromText('LINESTRING(15 15, 20 20)', 4326),
			ST_GeomFromText('POINT(15 20)', 4326),
			ST_GeomFromText('LINESTRING(0 0, 10 10, 20 25, 50 60)', 4326),
			ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0),(5 5, 7 5, 7 7, 5 7, 5 5))', 4326),
			ST_GeomFromText('MULTIPOINT(0 0, 20 20, 60 60)', 4326),
			ST_GeomFromText('MULTILINESTRING((10 10, 20 20), (15 15, 30 15))', 4326),
			ST_GeomFromText('MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)),((5 5, 7 5, 7 7, 5 7, 5 5)))', 4326),
			ST_GeomFromText('GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))', 4326)
		);`,
	}

	for _, request := range requests {
		rows, err := conn.QueryContext(context.Background(), request)
		require.NoError(t, err)
		require.NoError(t, rows.Close())
	}

	err = conn.Close()
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCountDifferentSchemas(t,
		Source.Database, Target.Database, "geo_test",
		helpers.GetSampleableStorageByModel(t, Source),
		helpers.GetSampleableStorageByModel(t, Target),
		60*time.Second))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
