package schemachange

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	pgx "github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	model "github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers/postgres"
	yt_provider "github.com/transferria/transferria/pkg/providers/yt"
	"github.com/transferria/transferria/pkg/runtime/local"
	"github.com/transferria/transferria/tests/helpers"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	sourceConnString = fmt.Sprintf(
		"host=localhost port=%d dbname=%s user=%s password=%s",
		sourcePort,
		os.Getenv("SOURCE_PG_LOCAL_DATABASE"),
		os.Getenv("SOURCE_PG_LOCAL_USER"),
		os.Getenv("SOURCE_PG_LOCAL_PASSWORD"),
	)
	sourcePort    = helpers.GetIntFromEnv("SOURCE_PG_LOCAL_PORT")
	targetCluster = os.Getenv("YT_PROXY")
)

func makeSource(tableName, slotID string) model.Source {
	src := &postgres.PgSource{
		Hosts:    []string{"localhost"},
		User:     os.Getenv("SOURCE_PG_LOCAL_USER"),
		Password: model.SecretString(os.Getenv("SOURCE_PG_LOCAL_PASSWORD")),
		Database: os.Getenv("SOURCE_PG_LOCAL_DATABASE"),
		Port:     sourcePort,
		DBTables: []string{tableName},
		SlotID:   slotID,
	}
	src.WithDefaults()
	return src
}

func makeTarget(namespace string) model.Destination {
	target := yt_provider.NewYtDestinationV1(yt_provider.YtDestination{
		Path:          fmt.Sprintf("//home/cdc/%s/pg2yt_e2e_schema_change", namespace),
		Cluster:       targetCluster,
		CellBundle:    "default",
		PrimaryMedium: "default",
	})
	target.WithDefaults()
	return target
}

type rowV1 struct {
	ID    int    `yson:"id"`
	Value string `yson:"value"`
}

type rowV2 struct {
	ID    int    `yson:"id"`
	Value string `yson:"value"`
	Extra string `yson:"extra"`
}

func TestSchemaChange(t *testing.T) {
	targetPort, err := helpers.GetPortFromStr(targetCluster)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: sourcePort},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	src := makeSource("public.test1", "slot1")
	dst := makeTarget("test1").(yt_provider.YtDestinationModel)

	transfer := &model.Transfer{
		ID:  "test1",
		Src: src,
		Dst: dst,
	}

	conn, err := pgx.Connect(context.Background(), sourceConnString)
	require.NoError(t, err)

	_, err = conn.Exec(context.Background(), `SELECT pg_create_logical_replication_slot('slot1', 'wal2json')`)
	require.NoError(t, err)
	defer conn.Exec(context.Background(), `SELECT pg_drop_replication_slot('slot1')`) //nolint

	w := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)

	errChan := make(chan error)
	go func() {
		errChan <- w.Run()
	}()

	_, err = conn.Exec(context.Background(), `INSERT INTO test1 VALUES (1, 'kek')`)
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), `INSERT INTO test1 VALUES (2, 'lel')`)
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), `INSERT INTO test1 VALUES (3, 'now i change the schema')`)
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "test1", helpers.GetSampleableStorageByModel(t, src), helpers.GetSampleableStorageByModel(t, dst.LegacyModel()), 60*time.Second))

	_, err = conn.Exec(context.Background(), `ALTER TABLE test1 ADD COLUMN extra TEXT;`)
	require.NoError(t, err)

	_, err = conn.Exec(context.Background(), `INSERT INTO test1 VALUES (4, 'schema changed, lol', 'four')`)
	require.NoError(t, err)

	err = <-errChan
	require.Error(t, err)
	require.Contains(t, err.Error(), "table schema has probably changed")
	err = w.Stop()
	require.NoError(t, err)

	r, err := ytEnv.YT.SelectRows(context.Background(), "* FROM [//home/cdc/test1/pg2yt_e2e_schema_change/test1]", nil)
	require.NoError(t, err)
	var ytTableDump []rowV1
	for r.Next() {
		var item rowV1
		require.NoError(t, r.Scan(&item))
		ytTableDump = append(ytTableDump, item)
	}
	require.Len(t, ytTableDump, 3)
	require.EqualValues(t, ytTableDump[0].ID, 1)
	require.EqualValues(t, ytTableDump[1].ID, 2)
	require.EqualValues(t, ytTableDump[2].ID, 3)
	require.EqualValues(t, ytTableDump[0].Value, "kek")
	require.EqualValues(t, ytTableDump[1].Value, "lel")
	require.EqualValues(t, ytTableDump[2].Value, "now i change the schema")
	err = r.Close()
	require.NoError(t, err)

	transfer = &model.Transfer{
		ID:  "test1",
		Src: makeSource("public.test1", "slot1"),
		Dst: makeTarget("test1"),
	}
	w = local.NewLocalWorker(coordinator.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)
	w.Start()
	defer w.Stop() //nolint

	_, err = conn.Exec(context.Background(), `INSERT INTO test1 VALUES (5, 'lmao', 'five')`)
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "test1", helpers.GetSampleableStorageByModel(t, src), helpers.GetSampleableStorageByModel(t, dst.LegacyModel()), 60*time.Second))

	r, err = ytEnv.YT.SelectRows(context.Background(), "* FROM [//home/cdc/test1/pg2yt_e2e_schema_change/test1] ORDER BY id ASC LIMIT 100", nil)
	require.NoError(t, err)
	defer r.Close()
	var ytTableDump2 []rowV2
	for r.Next() {
		var item rowV2
		require.NoError(t, r.Scan(&item))
		ytTableDump2 = append(ytTableDump2, item)
	}
	require.Len(t, ytTableDump2, 5)
	require.EqualValues(t, 1, ytTableDump2[0].ID)
	require.EqualValues(t, 2, ytTableDump2[1].ID)
	require.EqualValues(t, 3, ytTableDump2[2].ID)
	require.EqualValues(t, 4, ytTableDump2[3].ID)
	require.EqualValues(t, 5, ytTableDump2[4].ID)
	require.EqualValues(t, "kek", ytTableDump2[0].Value)
	require.EqualValues(t, "lel", ytTableDump2[1].Value)
	require.EqualValues(t, "now i change the schema", ytTableDump2[2].Value)
	require.EqualValues(t, "schema changed, lol", ytTableDump2[3].Value)
	require.EqualValues(t, "lmao", ytTableDump2[4].Value)
	require.EqualValues(t, "", ytTableDump2[0].Extra)
	require.EqualValues(t, "", ytTableDump2[1].Extra)
	require.EqualValues(t, "", ytTableDump2[2].Extra)
	require.EqualValues(t, "four", ytTableDump2[3].Extra)
	require.EqualValues(t, "five", ytTableDump2[4].Extra)
}

func TestNoSchemaNarrowingAttempted(t *testing.T) {
	targetPort, err := helpers.GetPortFromStr(targetCluster)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: sourcePort},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	_, err = ytEnv.YT.CreateNode(
		context.Background(),
		ypath.Path("//home/cdc/test2/pg2yt_e2e_schema_change/test2"),
		yt.NodeTable,
		&yt.CreateNodeOptions{
			Recursive: true,
			Attributes: map[string]interface{}{
				"dynamic": true,
				"schema": schema.Schema{
					UniqueKeys: true,
					Columns: []schema.Column{
						{
							Name:      "id",
							Type:      schema.TypeInt32,
							Required:  false,
							SortOrder: schema.SortAscending,
						}, {
							Name:     "value",
							Type:     schema.TypeString,
							Required: false,
						}, {
							Name:     "extra",
							Type:     schema.TypeString,
							Required: false,
						},
					},
				},
				"atomicity": "none",
			},
		},
	)
	require.NoError(t, err)

	src := makeSource("public.test2", "slot2")
	dst := makeTarget("test2")

	transfer := &model.Transfer{
		ID:  "test2",
		Src: src,
		Dst: dst,
	}

	conn, err := pgx.Connect(context.Background(), sourceConnString)
	require.NoError(t, err)

	_, err = conn.Exec(context.Background(), `SELECT pg_create_logical_replication_slot('slot2', 'wal2json')`)
	require.NoError(t, err)
	defer conn.Exec(context.Background(), `SELECT pg_drop_replication_slot('slot2')`) //nolint

	w := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)

	w.Start()
	defer w.Stop() //nolint

	_, err = conn.Exec(context.Background(), `INSERT INTO test2 VALUES (1, 'kek')`)
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), `INSERT INTO test2 VALUES (2, 'lel')`)
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "test2", helpers.GetSampleableStorageByModel(t, src), helpers.GetSampleableStorageByModel(t, dst.(yt_provider.YtDestinationModel).LegacyModel()), 60*time.Second))
}
