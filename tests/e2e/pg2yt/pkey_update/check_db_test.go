package pkeyupdate

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	yt_provider "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	ctx              = context.Background()
	sourceConnString = fmt.Sprintf(
		"host=localhost port=%d dbname=%s user=%s password=%s",
		helpers.GetIntFromEnv("SOURCE_PG_LOCAL_PORT"),
		os.Getenv("SOURCE_PG_LOCAL_DATABASE"),
		os.Getenv("SOURCE_PG_LOCAL_USER"),
		os.Getenv("SOURCE_PG_LOCAL_PASSWORD"),
	)
)

const (
	markerID    = 777
	markerValue = "marker"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

func makeSource() model.Source {
	src := &postgres.PgSource{
		Hosts:    []string{"localhost"},
		User:     os.Getenv("SOURCE_PG_LOCAL_USER"),
		Password: model.SecretString(os.Getenv("SOURCE_PG_LOCAL_PASSWORD")),
		Database: os.Getenv("SOURCE_PG_LOCAL_DATABASE"),
		Port:     helpers.GetIntFromEnv("SOURCE_PG_LOCAL_PORT"),
		DBTables: []string{"public.test"},
		SlotID:   "testslot",
	}
	src.WithDefaults()
	return src
}

func makeTarget(useStaticTableOnSnapshot bool) model.Destination {
	target := yt_provider.NewYtDestinationV1(yt_provider.YtDestination{
		Path:                     "//home/cdc/pg2yt_e2e_pkey_change",
		Cluster:                  os.Getenv("YT_PROXY"),
		CellBundle:               "default",
		PrimaryMedium:            "default",
		UseStaticTableOnSnapshot: useStaticTableOnSnapshot,
	})
	target.WithDefaults()
	return target
}

type row struct {
	ID     int    `yson:"id"`
	IdxCol int    `yson:"idxcol"`
	Value  string `yson:"value"`
}

func exec(t *testing.T, conn *pgx.Conn, query string) {
	_, err := conn.Exec(ctx, query)
	require.NoError(t, err)
}

type fixture struct {
	t            *testing.T
	transfer     *model.Transfer
	ytEnv        *yttest.Env
	destroyYtEnv func()
}

func (f *fixture) teardown() {
	forceRemove := &yt.RemoveNodeOptions{Force: true}
	err := f.ytEnv.YT.RemoveNode(ctx, ypath.Path("//home/cdc/pg2yt_e2e_pkey_change/test"), forceRemove)
	require.NoError(f.t, err)
	err = f.ytEnv.YT.RemoveNode(ctx, ypath.Path("//home/cdc/pg2yt_e2e_pkey_change/test__idx_idxcol"), forceRemove)
	require.NoError(f.t, err)
	f.destroyYtEnv()

	conn, err := pgx.Connect(context.Background(), sourceConnString)
	require.NoError(f.t, err)
	defer conn.Close(context.Background())

	exec(f.t, conn, `DROP TABLE public.test`)
	exec(f.t, conn, `SELECT pg_drop_replication_slot('testslot')`)
}

func setup(t *testing.T, useStaticTableOnSnapshot bool) *fixture {
	ytEnv, destroyYtEnv := yttest.NewEnv(t)

	conn, err := pgx.Connect(context.Background(), sourceConnString)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	exec(t, conn, `CREATE TABLE public.test (id INTEGER PRIMARY KEY, idxcol INTEGER NOT NULL, value TEXT)`)
	exec(t, conn, `ALTER TABLE public.test ALTER COLUMN value SET STORAGE EXTERNAL`)
	exec(t, conn, `INSERT INTO public.test VALUES (1, 10, 'kek')`)
	exec(t, conn, `SELECT pg_create_logical_replication_slot('testslot', 'wal2json')`)

	target := makeTarget(useStaticTableOnSnapshot)
	return &fixture{
		t:            t,
		transfer:     helpers.MakeTransfer(helpers.TransferID, makeSource(), target, abstract.TransferTypeSnapshotAndIncrement),
		ytEnv:        ytEnv,
		destroyYtEnv: destroyYtEnv,
	}
}

func (f *fixture) update(value string) {
	conn, err := pgx.Connect(context.Background(), sourceConnString)
	require.NoError(f.t, err)
	defer conn.Close(context.Background())

	exec(f.t, conn, fmt.Sprintf(`UPDATE public.test SET id = 2, value = '%s' WHERE id = 1`, value))
	exec(f.t, conn, fmt.Sprintf(`INSERT INTO public.test VALUES (%d, %d, '%s')`, markerID, markerID*10, markerValue))
}

func (f *fixture) checkTableAfterUpdate(value string) {
	if diff := cmp.Diff(
		f.readAll("//home/cdc/pg2yt_e2e_pkey_change/test"),
		[]row{
			{ID: 2, IdxCol: 10, Value: value},
			{ID: markerID, IdxCol: markerID * 10, Value: markerValue},
		},
	); diff != "" {
		require.Fail(f.t, "Tables do not match", "Diff:\n%s", diff)
	}
}

func (f *fixture) readAll(tablePath string) (result []row) {
	reader, err := f.ytEnv.YT.SelectRows(ctx, fmt.Sprintf("* FROM [%s]", tablePath), &yt.SelectRowsOptions{})
	require.NoError(f.t, err)
	defer reader.Close()

	for reader.Next() {
		var row row
		require.NoError(f.t, reader.Scan(&row))
		result = append(result, row)
	}
	require.NoError(f.t, reader.Err())
	return
}

func (f *fixture) readAllIndex(tablePath string) (result []idxRow) {
	reader, err := f.ytEnv.YT.SelectRows(ctx, fmt.Sprintf("* FROM [%s]", tablePath), &yt.SelectRowsOptions{})
	require.NoError(f.t, err)
	defer reader.Close()

	for reader.Next() {
		var idxRow idxRow
		require.NoError(f.t, reader.Scan(&idxRow))
		result = append(result, idxRow)
	}
	require.NoError(f.t, reader.Err())
	return
}

func (f *fixture) waitMarker() {
	for {
		reader, err := f.ytEnv.YT.LookupRows(
			ctx,
			ypath.Path("//home/cdc/pg2yt_e2e_pkey_change/test"),
			[]interface{}{map[string]int{"id": markerID}},
			&yt.LookupRowsOptions{},
		)
		require.NoError(f.t, err)
		if !reader.Next() {
			time.Sleep(100 * time.Millisecond)
			_ = reader.Close()
			continue
		}

		defer reader.Close()
		var row row
		require.NoError(f.t, reader.Scan(&row))
		require.False(f.t, reader.Next())
		require.EqualValues(f.t, markerID, row.ID)
		require.EqualValues(f.t, markerValue, row.Value)
		return
	}
}

func (f *fixture) loadAndCheckSnapshot(cp coordinator.Coordinator) {
	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewStatefulFakeClient(), "test-operation", f.transfer, helpers.EmptyRegistry())
	err := snapshotLoader.LoadSnapshot(ctx)
	require.NoError(f.t, err)

	if diff := cmp.Diff(
		f.readAll("//home/cdc/pg2yt_e2e_pkey_change/test"),
		[]row{{ID: 1, IdxCol: 10, Value: "kek"}},
	); diff != "" {
		require.Fail(f.t, "Tables do not match", "Diff:\n%s", diff)
	}
}

func srcAndDstPorts(fxt *fixture) (int, int, error) {
	sourcePort := fxt.transfer.Src.(*postgres.PgSource).Port
	ytCluster := fxt.transfer.Dst.(yt_provider.YtDestinationModel).Cluster()
	targetPort, err := helpers.GetPortFromStr(ytCluster)
	if err != nil {
		return 1, 1, err
	}
	return sourcePort, targetPort, err
}

func TestPkeyUpdate(t *testing.T) {
	fixture := setup(t, true)

	sourcePort, targetPort, err := srcAndDstPorts(fixture)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: sourcePort},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	defer fixture.teardown()

	fakeClient := coordinator.NewStatefulFakeClient()
	fixture.loadAndCheckSnapshot(fakeClient)

	workerErrChannel := make(chan error)
	defer func() { require.NoError(t, <-workerErrChannel) }()

	w := local.NewLocalWorker(fakeClient, fixture.transfer, helpers.EmptyRegistry(), logger.Log)
	defer func() { require.NoError(t, w.Stop()) }()

	go func() { workerErrChannel <- w.Run() }()

	fixture.update("lel")
	fixture.waitMarker()
	fixture.checkTableAfterUpdate("lel")
}

type idxRow struct {
	IdxCol int         `yson:"idxcol"`
	ID     int         `yson:"id"`
	Dummy  interface{} `yson:"_dummy"`
}

func TestPkeyUpdateIndex(t *testing.T) {
	fixture := setup(
		t,
		false, // TM-4381
	)

	sourcePort, targetPort, err := srcAndDstPorts(fixture)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: sourcePort},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	defer fixture.teardown()

	fixture.transfer.Dst.(yt_provider.YtDestinationModel).SetIndex([]string{"idxcol"})

	fixture.loadAndCheckSnapshot(coordinator.NewFakeClient())

	idxTablePath := "//home/cdc/pg2yt_e2e_pkey_change/test__idx_idxcol"
	if diff := cmp.Diff([]idxRow{{IdxCol: 10, ID: 1}}, fixture.readAllIndex(idxTablePath)); diff != "" {
		require.Fail(t, "Tables do not match", "Diff:\n%s", diff)
	}

	workerErrChannel := make(chan error)
	defer func() { require.NoError(t, <-workerErrChannel) }()

	w := local.NewLocalWorker(coordinator.NewFakeClient(), fixture.transfer, helpers.EmptyRegistry(), logger.Log)
	defer func() { require.NoError(t, w.Stop()) }()

	go func() { workerErrChannel <- w.Run() }()

	fixture.update("lel")
	fixture.waitMarker()
	fixture.checkTableAfterUpdate("lel")

	if diff := cmp.Diff(
		[]idxRow{{IdxCol: 10, ID: 2}, {IdxCol: markerID * 10, ID: markerID}},
		fixture.readAllIndex(idxTablePath),
	); diff != "" {
		require.Fail(t, "Tables do not match", "Diff:\n%s", diff)
	}
}

func TestPkeyUpdateIndexToast(t *testing.T) {
	fixture := setup(
		t,
		false, // TM-4381
	)

	sourcePort, targetPort, err := srcAndDstPorts(fixture)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: sourcePort},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	defer fixture.teardown()

	fixture.transfer.Dst.(yt_provider.YtDestinationModel).SetIndex([]string{"idxcol"})

	fixture.loadAndCheckSnapshot(coordinator.NewFakeClient())

	idxTablePath := "//home/cdc/pg2yt_e2e_pkey_change/test__idx_idxcol"
	if diff := cmp.Diff([]idxRow{{IdxCol: 10, ID: 1}}, fixture.readAllIndex(idxTablePath)); diff != "" {
		require.Fail(t, "Tables do not match", "Diff:\n%s", diff)
	}

	workerErrChannel := make(chan error)
	defer func() { require.NoError(t, <-workerErrChannel) }()

	w := local.NewLocalWorker(coordinator.NewFakeClient(), fixture.transfer, helpers.EmptyRegistry(), logger.Log)
	defer func() { require.NoError(t, w.Stop()) }()

	go func() { workerErrChannel <- w.Run() }()

	longString := strings.Repeat("x", 32000)
	fixture.update(longString)
	fixture.waitMarker()
	fixture.checkTableAfterUpdate(longString)

	if diff := cmp.Diff(
		[]idxRow{{IdxCol: 10, ID: 2}, {IdxCol: markerID * 10, ID: markerID}},
		fixture.readAllIndex(idxTablePath),
	); diff != "" {
		require.Fail(t, "Tables do not match", "Diff:\n%s", diff)
	}
}
