package index

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
	yt_sink "github.com/transferia/transferia/pkg/providers/yt/sink"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/util"
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

var markerIdx = fmt.Sprintf("%d", markerID*10)

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

func makeTarget(idxs []string) model.Destination {
	target := yt_provider.NewYtDestinationV1(yt_provider.YtDestination{
		Path:                     "//home/cdc/pg2yt_e2e_index",
		Cluster:                  os.Getenv("YT_PROXY"),
		CellBundle:               "default",
		PrimaryMedium:            "default",
		Index:                    idxs,
		UseStaticTableOnSnapshot: true, // TM-4381
	})
	target.WithDefaults()
	return target
}

type row struct {
	ID     int    `yson:"id"`
	IdxCol string `yson:"idxcol"`
	Value  string `yson:"value"`
}

func (f *fixture) exec(query string) {
	_, err := f.pgConn.Exec(ctx, query)
	require.NoError(f.t, err)
}

type fixture struct {
	t            *testing.T
	transfer     *model.Transfer
	ytEnv        *yttest.Env
	pgConn       *pgx.Conn
	destroyYtEnv func()
	wrk          *local.LocalWorker
	workerCh     chan error
	markerKey    map[string]interface{}
}

func (f *fixture) teardown() {
	require.NoError(f.t, f.wrk.Stop())
	require.NoError(f.t, <-f.workerCh)

	forceRemove := &yt.RemoveNodeOptions{Force: true}
	err := f.ytEnv.YT.RemoveNode(ctx, ypath.Path("//home/cdc/pg2yt_e2e_index/test"), forceRemove)
	require.NoError(f.t, err)
	err = f.ytEnv.YT.RemoveNode(ctx, ypath.Path("//home/cdc/pg2yt_e2e_index/test__idx_idxcol"), forceRemove)
	require.NoError(f.t, err)
	f.destroyYtEnv()

	f.exec(`DROP TABLE public.test`)
	f.exec(`SELECT pg_drop_replication_slot('testslot')`)
	require.NoError(f.t, f.pgConn.Close(context.Background()))
}

func setup(t *testing.T, markerKey map[string]interface{}, idxs []string) *fixture {
	ytEnv, destroyYtEnv := yttest.NewEnv(t)

	var rollbacks util.Rollbacks
	defer rollbacks.Do()
	pgConn, err := pgx.Connect(context.Background(), sourceConnString)
	require.NoError(t, err)
	rollbacks.Add(func() { require.NoError(t, pgConn.Close(context.Background())) })

	transfer := helpers.MakeTransfer(helpers.TransferID, makeSource(), makeTarget(idxs), abstract.TransferTypeSnapshotAndIncrement)
	wrk := local.NewLocalWorker(coordinator.NewStatefulFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)

	f := &fixture{
		t:            t,
		transfer:     transfer,
		ytEnv:        ytEnv,
		destroyYtEnv: destroyYtEnv,
		pgConn:       pgConn,
		workerCh:     make(chan error),
		wrk:          wrk,
		markerKey:    markerKey,
	}

	insertInitialContent := `
		INSERT INTO public.test VALUES
			(1, 'one', 'The one'),
			(2, 'two', 'The two'),
			(3, 'three', 'The three')`

	primaryKeys := []string{}
	for k := range markerKey {
		primaryKeys = append(primaryKeys, k)
	}
	f.exec(`CREATE TABLE public.test (id INTEGER, idxcol TEXT, value TEXT)`)
	f.exec(fmt.Sprintf(`ALTER TABLE public.test ADD PRIMARY KEY (%s)`, strings.Join(primaryKeys, ", ")))
	f.exec(`ALTER TABLE public.test ALTER COLUMN idxcol SET STORAGE EXTERNAL`)
	f.exec(`ALTER TABLE public.test ALTER COLUMN value SET STORAGE EXTERNAL`)
	f.exec(insertInitialContent)
	f.exec(`SELECT pg_create_logical_replication_slot('testslot', 'wal2json')`)

	f.loadAndCheckSnapshot()

	go func() { f.workerCh <- f.wrk.Run() }()

	rollbacks.Cancel()
	return f
}

func (f *fixture) insertMarker() {
	f.exec(fmt.Sprintf(`INSERT INTO public.test VALUES (%d, '%s', '%s')`, markerID, markerIdx, markerValue))
}

func (f *fixture) requireEmptyDiff(diff string) {
	if diff != "" {
		require.Fail(f.t, "Tables do not match", "Diff:\n%s", diff)
	}
}

func (f *fixture) readAll() (result []row) {
	reader, err := f.ytEnv.YT.SelectRows(ctx, `* FROM [//home/cdc/pg2yt_e2e_index/test] ORDER BY id ASC LIMIT 100`, &yt.SelectRowsOptions{})
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

func (f *fixture) readAllIndex(colName string) (result []any) {
	reader, err := f.ytEnv.YT.SelectRows(ctx, fmt.Sprintf(`* FROM [//home/cdc/pg2yt_e2e_index/test__idx_%s] ORDER BY id ASC LIMIT 100`, colName), &yt.SelectRowsOptions{})
	require.NoError(f.t, err)
	defer reader.Close()

	for reader.Next() {
		var idxRow map[string]any
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
			ypath.Path("//home/cdc/pg2yt_e2e_index/test"),
			[]interface{}{f.markerKey},
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

func (f *fixture) loadAndCheckSnapshot() {
	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewStatefulFakeClient(), "test-operation", f.transfer, helpers.EmptyRegistry())
	err := snapshotLoader.LoadSnapshot(ctx)
	require.NoError(f.t, err)

	if diff := cmp.Diff(
		f.readAll(),
		[]row{
			{ID: 1, IdxCol: "one", Value: "The one"},
			{ID: 2, IdxCol: "two", Value: "The two"},
			{ID: 3, IdxCol: "three", Value: "The three"},
		},
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

func TestIndexBasic(t *testing.T) {
	fixture := setup(t, map[string]interface{}{"id": markerID}, []string{"idxcol"})

	sourcePort, targetPort, err := srcAndDstPorts(fixture)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: sourcePort},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	defer fixture.teardown()

	fixture.exec(`UPDATE public.test SET id = 10 WHERE id = 1`)
	fixture.exec(`UPDATE public.test SET idxcol = 'TWO' WHERE idxcol = 'two'`)
	fixture.insertMarker()
	fixture.waitMarker()

	fixture.requireEmptyDiff(cmp.Diff(
		[]row{
			{ID: 2, IdxCol: "TWO", Value: "The two"},
			{ID: 3, IdxCol: "three", Value: "The three"},
			{ID: 10, IdxCol: "one", Value: "The one"},
			{ID: markerID, IdxCol: markerIdx, Value: markerValue},
		},
		fixture.readAll(),
	))
	fixture.requireEmptyDiff(cmp.Diff(
		[]any{
			map[string]any{"_dummy": nil, "id": int64(2), "idxcol": "TWO"},
			map[string]any{"_dummy": nil, "id": int64(3), "idxcol": "three"},
			map[string]any{"_dummy": nil, "id": int64(10), "idxcol": "one"},
			map[string]any{"_dummy": nil, "id": int64(markerID), "idxcol": markerIdx},
		},
		fixture.readAllIndex("idxcol"),
	))
}

func TestIndexMany(t *testing.T) {
	fixture := setup(t, map[string]interface{}{"id": markerID}, []string{"idxcol", "value"})

	sourcePort, targetPort, err := srcAndDstPorts(fixture)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: sourcePort},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	defer fixture.teardown()

	fixture.exec(`UPDATE public.test SET id = 10 WHERE id = 1`)
	fixture.exec(`UPDATE public.test SET idxcol = 'TWO' WHERE idxcol = 'two'`)
	fixture.insertMarker()
	fixture.waitMarker()

	fixture.requireEmptyDiff(cmp.Diff(
		[]row{
			{ID: 2, IdxCol: "TWO", Value: "The two"},
			{ID: 3, IdxCol: "three", Value: "The three"},
			{ID: 10, IdxCol: "one", Value: "The one"},
			{ID: markerID, IdxCol: markerIdx, Value: markerValue},
		},
		fixture.readAll(),
	))
	fixture.requireEmptyDiff(cmp.Diff(
		[]any{
			map[string]any{"_dummy": nil, "id": int64(2), "idxcol": "TWO"},
			map[string]any{"_dummy": nil, "id": int64(3), "idxcol": "three"},
			map[string]any{"_dummy": nil, "id": int64(10), "idxcol": "one"},
			map[string]any{"_dummy": nil, "id": int64(markerID), "idxcol": markerIdx},
		},
		fixture.readAllIndex("idxcol"),
	))
	fixture.requireEmptyDiff(cmp.Diff(
		[]any{
			map[string]any{"_dummy": nil, "id": int64(2), "value": "The two"},
			map[string]any{"_dummy": nil, "id": int64(3), "value": "The three"},
			map[string]any{"_dummy": nil, "id": int64(10), "value": "The one"},
			map[string]any{"_dummy": nil, "id": int64(markerID), "value": markerValue},
		},
		fixture.readAllIndex("value"),
	))
}

func TestIndexToast(t *testing.T) {
	fixture := setup(t, map[string]interface{}{"id": markerID}, []string{"idxcol"})

	sourcePort, targetPort, err := srcAndDstPorts(fixture)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: sourcePort},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	defer fixture.teardown()

	fixture.exec(fmt.Sprintf(`UPDATE public.test SET idxcol = '%s' WHERE id = 2`, strings.Repeat("x", 64*1024)))
	fixture.insertMarker()
	fixture.waitMarker()

	fixture.requireEmptyDiff(cmp.Diff(
		[]any{
			map[string]any{"_dummy": nil, "id": int64(1), "idxcol": "one"},
			map[string]any{"_dummy": nil, "id": int64(2), "idxcol": strings.Repeat("x", 64*1024)},
			map[string]any{"_dummy": nil, "id": int64(3), "idxcol": "three"},
			map[string]any{"_dummy": nil, "id": int64(markerID), "idxcol": markerIdx},
		},
		fixture.readAllIndex("idxcol"),
	))
}

func TestIndexPrimaryKey(t *testing.T) {
	fixture := setup(t, map[string]interface{}{"id": markerID, "idxcol": markerIdx}, []string{"idxcol"})

	sourcePort, targetPort, err := srcAndDstPorts(fixture)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: sourcePort},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	defer fixture.teardown()

	fixture.exec(`UPDATE public.test SET idxcol = 'ONE' WHERE id = 1`)
	fixture.insertMarker()
	fixture.waitMarker()

	fixture.requireEmptyDiff(cmp.Diff(
		[]any{
			map[string]any{"_dummy": nil, "id": int64(1), "idxcol": "ONE"},
			map[string]any{"_dummy": nil, "id": int64(2), "idxcol": "two"},
			map[string]any{"_dummy": nil, "id": int64(3), "idxcol": "three"},
			map[string]any{"_dummy": nil, "id": int64(markerID), "idxcol": markerIdx},
		},
		fixture.readAllIndex("idxcol"),
	))
}

func TestSkipLongStrings(t *testing.T) {
	fixture := setup(t, map[string]interface{}{"id": markerID}, []string{"idxcol"})
	fixture.transfer.Dst.(*yt_provider.YtDestinationWrapper).Model.DiscardBigValues = true

	sourcePort, targetPort, err := srcAndDstPorts(fixture)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: sourcePort},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	defer fixture.teardown()

	fixture.exec(fmt.Sprintf(`INSERT INTO public.test VALUES (4, 'four', '%s')`, strings.Repeat("x", 16*1024*1024+1)))
	fixture.insertMarker()
	fixture.waitMarker()

	fixture.requireEmptyDiff(cmp.Diff(
		[]any{
			map[string]any{"_dummy": nil, "id": int64(1), "idxcol": "one"},
			map[string]any{"_dummy": nil, "id": int64(2), "idxcol": "two"},
			map[string]any{"_dummy": nil, "id": int64(3), "idxcol": "three"},
			map[string]any{"_dummy": nil, "id": int64(4), "idxcol": "four"},
			map[string]any{"_dummy": nil, "id": int64(markerID), "idxcol": markerIdx},
		},
		fixture.readAllIndex("idxcol"),
	))

	fixture.requireEmptyDiff(cmp.Diff(
		[]row{
			{IdxCol: "one", ID: 1, Value: "The one"},
			{IdxCol: "two", ID: 2, Value: "The two"},
			{IdxCol: "three", ID: 3, Value: "The three"},
			{IdxCol: "four", ID: 4, Value: yt_sink.MagicString},
			{IdxCol: markerIdx, ID: markerID, Value: markerValue},
		},
		fixture.readAll(),
	))
}

func TestDelete(t *testing.T) {
	fixture := setup(t, map[string]interface{}{"id": markerID}, []string{"idxcol"})

	sourcePort, targetPort, err := srcAndDstPorts(fixture)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: sourcePort},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	defer fixture.teardown()

	fixture.exec(`DELETE FROM public.test WHERE id < 3`)
	fixture.insertMarker()
	fixture.waitMarker()

	fixture.requireEmptyDiff(cmp.Diff(
		[]any{
			map[string]any{"_dummy": nil, "id": int64(3), "idxcol": "three"},
			map[string]any{"_dummy": nil, "id": int64(markerID), "idxcol": markerIdx},
		},
		fixture.readAllIndex("idxcol"),
	))
}
