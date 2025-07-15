package alters

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
	"go.ytsaurus.tech/yt/go/ypath"
	ytMain "go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	Source = *helpers.WithMysqlInclude(helpers.RecipeMysqlSource(), []string{"__test_a", "__test_b", "__test_c", "__test_d"})
	Target = yt_helpers.RecipeYtTarget("//home/cdc/test/mysql2yt_e2e_alters")
)

func init() {
	Source.WithDefaults()
}

func makeConnConfig() *mysqlDriver.Config {
	cfg := mysqlDriver.NewConfig()
	cfg.Addr = fmt.Sprintf("%v:%v", Source.Host, Source.Port)
	cfg.User = Source.User
	cfg.Passwd = string(Source.Password)
	cfg.DBName = Source.Database
	cfg.Net = "tcp"
	cfg.MultiStatements = true
	return cfg
}

func TestGroup(t *testing.T) {
	targetPort, err := helpers.GetPortFromStr(Target.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: Source.Port},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	ctx := context.Background()

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path("//home/cdc/test/mysql2yt_e2e_alters"), ytMain.NodeMap, &ytMain.CreateNodeOptions{Recursive: true})
	defer func() {
		err := ytEnv.YT.RemoveNode(ctx, ypath.Path("//home/cdc/test/mysql2yt_e2e_alters"), &ytMain.RemoveNodeOptions{Recursive: true})
		require.NoError(t, err)
	}()
	require.NoError(t, err)

	Target.AllowAlter()
	t.Run("Load", Load)
}

func Load(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotAndIncrement)

	ctx := context.Background()

	conn, err := mysqlDriver.NewConnector(makeConnConfig())
	require.NoError(t, err)
	db := sql.OpenDB(conn)

	initInserts := `
drop table if exists __test_a;
drop table if exists __test_b;
drop table if exists __test_c;
drop table if exists __test_d;

create table __test_a
(
    a_id   integer      not null primary key,
    a_name varchar(255) not null
) engine = innodb
  default charset = utf8;

create table __test_b
(
    b_id      integer      not null primary key,
    b_name    varchar(255) not null,
    b_address varchar(255) not null
) engine = innodb
  default charset = utf8;

create table __test_c
(
    c_id   integer      not null primary key,
    c_uid  integer      not null,
    c_name varchar(255) not null
) engine = innodb
  default charset = utf8;

create table __test_d
(
    d_id   int not null primary key,
    d_uid  bigint,
    d_name varchar(255)
) engine = innodb
  default charset = utf8;

insert into __test_a (a_id, a_name)
values (1, 'jagajaga'),
       (2, 'bamboo');

insert into __test_b (b_id, b_name, b_address)
values (1, 'Mike', 'Pushkinskaya, 1'),
       (2, 'Rafael', 'Ostankinskaya, 8');

insert into __test_c (c_id, c_uid, c_name)
values (1, 9, 'Macbook Pro, 15'),
       (2, 4, 'HP Pavilion');

insert into __test_d (d_id, d_uid, d_name)
values (1, 13, 'Reverse Engineering'),
       (2, 37, 'Evolutionary Computations');
`
	_, err = db.Exec(initInserts)
	require.NoError(t, err)

	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	err = snapshotLoader.LoadSnapshot(ctx)
	require.NoError(t, err)

	fakeClient := coordinator.NewStatefulFakeClient()
	err = mysql.SyncBinlogPosition(&Source, transfer.ID, fakeClient)
	require.NoError(t, err)

	wrk := local.NewLocalWorker(fakeClient, transfer, helpers.EmptyRegistry(), logger.Log)

	workerErrCh := make(chan error)
	go func() {
		workerErrCh <- wrk.Run()
	}()

	//------------------------------------------------------------------------------

	insertBeforeA := "INSERT INTO `__test_a` (a_id, a_name) VALUES (3, 'Bee for ALTER');"
	_, err = db.Exec(insertBeforeA)
	require.NoError(t, err)

	insertBeforeB := "INSERT INTO `__test_b` (b_id, b_name, b_address) VALUES (3, 'Rachel', 'Baker Street, 2');"
	_, err = db.Exec(insertBeforeB)
	require.NoError(t, err)

	insertBeforeC := "INSERT INTO `__test_c` (c_id, c_uid, c_name) VALUES (3, 48, 'Dell GTX-5667');"
	_, err = db.Exec(insertBeforeC)
	require.NoError(t, err)

	insertBeforeD := "INSERT INTO `__test_d` (d_id, d_uid, d_name) VALUES (3, 34, 'Distributed Systems');"
	_, err = db.Exec(insertBeforeD)
	require.NoError(t, err)

	var checkSourceRowCount int
	rowsNumberA := "SELECT SUM(1) FROM `__test_a`"
	err = db.QueryRow(rowsNumberA).Scan(&checkSourceRowCount)
	require.NoError(t, err)
	require.Equal(t, 3, checkSourceRowCount)

	rowsNumberB := "SELECT SUM(1) FROM `__test_b`"
	err = db.QueryRow(rowsNumberB).Scan(&checkSourceRowCount)
	require.NoError(t, err)
	require.Equal(t, 3, checkSourceRowCount)

	rowsNumberC := "SELECT SUM(1) FROM `__test_c`"
	err = db.QueryRow(rowsNumberC).Scan(&checkSourceRowCount)
	require.NoError(t, err)
	require.Equal(t, 3, checkSourceRowCount)

	rowsNumberD := "SELECT SUM(1) FROM `__test_d`"
	err = db.QueryRow(rowsNumberD).Scan(&checkSourceRowCount)
	require.NoError(t, err)
	require.Equal(t, 3, checkSourceRowCount)

	//------------------------------------------------------------------------------

	require.NoError(t, helpers.WaitEqualRowsCount(t, Source.Database, "__test_a", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second))

	//------------------------------------------------------------------------------

	alterRequestA := "ALTER TABLE `__test_a` ADD a_current_time TIMESTAMP;"
	_, err = db.Exec(alterRequestA)
	require.NoError(t, err)

	alterRequestB := "ALTER TABLE `__test_b` DROP COLUMN b_address;"
	_, err = db.Exec(alterRequestB)
	require.NoError(t, err)

	alterRequestC := "ALTER TABLE `__test_c` DROP COLUMN c_uid;"
	_, err = db.Exec(alterRequestC)
	require.NoError(t, err)

	alterRequestExtensionD := "ALTER TABLE `__test_d` MODIFY d_id bigint NOT NULL;"
	_, err = db.Exec(alterRequestExtensionD)
	require.NoError(t, err)

	alterRequestNarrowingD := "ALTER TABLE `__test_d` MODIFY d_uid int;"
	_, err = db.Exec(alterRequestNarrowingD)
	require.NoError(t, err)

	var checkTypeD string
	requestCheckTypeD := "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '__test_d' AND COLUMN_NAME = 'd_uid'"
	err = db.QueryRow(requestCheckTypeD).Scan(&checkTypeD)
	require.NoError(t, err)
	require.Equal(t, "int", checkTypeD)

	// ---------------------------------------------------------------------

	insertAfterA := "INSERT INTO `__test_a` (a_id, a_name, a_current_time) VALUES (4, 'Happy Tester', now());"
	_, err = db.Exec(insertAfterA)
	require.NoError(t, err)

	insertAfterB := "INSERT INTO `__test_b` (b_id, b_name) VALUES (4, 'Katrin');"
	_, err = db.Exec(insertAfterB)
	require.NoError(t, err)

	insertAfterC := "INSERT INTO `__test_c` (c_id, c_name) VALUES (4, 'Lenovo ThinkPad Pro');"
	_, err = db.Exec(insertAfterC)
	require.NoError(t, err)

	requestCorrectD := "INSERT INTO `__test_d` (d_id, d_uid, d_name) VALUES (2147483648, 0, 'Joseph');"
	_, err = db.Exec(requestCorrectD)
	require.NoError(t, err)

	// Enables strict SQL mode and an out of range error occurs while inserting bigger or smaller value than supported
	changeOverflowBehaviour := "SET SESSION sql_mode = 'TRADITIONAL';"
	_, err = db.ExecContext(context.Background(), changeOverflowBehaviour)
	require.NoError(t, err)

	requestIncorrectD := "INSERT INTO `__test_d` (d_id, d_uid, d_name) VALUES (1337, 2147483648, 'Alex');"
	_, err = db.Exec(requestIncorrectD)
	require.Error(t, err)

	err = db.Close()
	require.NoError(t, err)

	// ---------------------------------------------------------------------

	require.NoError(t, helpers.WaitEqualRowsCount(t, Source.Database, "__test_a", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, Source.Database, "__test_b", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, Source.Database, "__test_c", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, Source.Database, "__test_d", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second))
}
