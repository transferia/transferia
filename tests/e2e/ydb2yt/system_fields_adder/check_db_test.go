package main

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	yt_storage "github.com/transferia/transferia/pkg/providers/yt/storage"
	"github.com/transferia/transferia/pkg/providers/yt/yt_client"
	_ "github.com/transferia/transferia/pkg/transformer/registry/system_fields_adder"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/ydb"
	_ "github.com/transferia/transferia/tests/helpers/registration/yt"
	"github.com/transferia/transferia/tests/helpers/testenv"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

const tableName = "system_fields_adder_snapshot"

func TestSnapshot(t *testing.T) {
	src := &provider_ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           testenv.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           testenv.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             []string{tableName},
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
	}
	dst := provider_yt.NewYtDestinationV1(provider_yt.YtDestination{
		Path:                     "//home/cdc/test/ydb2yt_snapshot_system_fields_adder",
		Cluster:                  os.Getenv("YT_PROXY"),
		CellBundle:               "default",
		PrimaryMedium:            "default",
		UseStaticTableOnSnapshot: true, // TM-4444
	})

	sourcePort, err := network.GetPortFromStr(src.Instance)
	require.NoError(t, err)
	targetPort, err := network.GetPortFromStr(dst.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "YDB source", Port: sourcePort},
			network.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	transferhelpers.InitSrcDst(transferhelpers.TransferID, src, dst, abstract.TransferTypeSnapshotOnly)

	t.Run("seed data", func(t *testing.T) {
		target := &provider_ydb.YdbDestination{
			Database: src.Database,
			Token:    src.Token,
			Instance: src.Instance,
		}
		target.WithDefaults()
		sinker, err := provider_ydb.NewSinker(logger.Log, target, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)
		testSchema := abstract.NewTableSchema([]abstract.ColSchema{
			{ColumnName: "id", DataType: string(ytschema.TypeInt32), PrimaryKey: true},
			{ColumnName: "val", DataType: string(ytschema.TypeString)},
		})
		require.NoError(t, sinker.Push([]abstract.ChangeItem{{
			Kind:         abstract.InsertKind,
			Schema:       "",
			Table:        tableName,
			ColumnNames:  []string{"id", "val"},
			ColumnValues: []interface{}{1, "test"},
			TableSchema:  testSchema,
		}}))
	})

	t.Run("activate transfer", func(t *testing.T) {
		transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, src, dst, abstract.TransferTypeSnapshotOnly)
		require.NoError(t, transfer.TransformationFromJSON(`
{
  "transformers": [
    {
      "systemFieldsAdder": {
        "Fields": [
          { "FieldType": "id", "ColumnName": "__dt_id" },
          { "FieldType": "lsn", "ColumnName": "__dt_lsn" },
          { "FieldType": "tx_position", "ColumnName": "__dt_tx_position" },
          { "FieldType": "commit_time", "ColumnName": "__dt_commit_time" },
          { "FieldType": "tx_id", "ColumnName": "__dt_tx_id" }
        ],
        "Tables": {
          "includeTables": [ "^system_fields_adder_snapshot$" ]
        }
      }
    }
  ]
}`))
		require.NoError(t, tasks.ActivateDelivery(context.TODO(), nil, coordinator.NewStatefulFakeClient(), *transfer, testmetrics.EmptyRegistry()))
	})

	t.Run("check data", func(t *testing.T) {
		config := new(yt.Config)
		client, err := yt_client.NewYtClientWrapper(yt_client.HTTP, nil, config)
		require.NoError(t, err)

		tablePath := ypath.Path(dst.Path()).Child(tableName)
		var dynamic bool
		require.NoError(t, client.GetNode(context.Background(), tablePath.Attr("dynamic"), &dynamic, nil))
		require.True(t, dynamic)

		st, err := yt_storage.NewStorage(&provider_yt.YtStorageParams{
			Token:   dst.Token(),
			Cluster: os.Getenv("YT_PROXY"),
			Path:    dst.Path(),
			Spec:    nil,
		})
		require.NoError(t, err)

		schema, err := st.TableSchema(context.Background(), abstract.TableID{Name: tableName})
		require.NoError(t, err)
		require.NotNil(t, schema)
		colsByName := map[string]abstract.ColSchema{}
		for _, col := range schema.Columns() {
			colsByName[col.ColumnName] = col
		}
		require.Equal(t, string(ytschema.TypeInt32), colsByName["id"].DataType)
		require.True(t, colsByName["id"].PrimaryKey)
		require.Equal(t, string(ytschema.TypeString), colsByName["val"].DataType)
		require.Equal(t, string(ytschema.TypeUint64), colsByName["__dt_id"].DataType)
		require.Equal(t, string(ytschema.TypeUint64), colsByName["__dt_lsn"].DataType)
		require.Equal(t, string(ytschema.TypeInt64), colsByName["__dt_tx_position"].DataType)
		require.Equal(t, string(ytschema.TypeUint64), colsByName["__dt_commit_time"].DataType)
		require.Equal(t, string(ytschema.TypeString), colsByName["__dt_tx_id"].DataType)
		for _, name := range []string{"__dt_id", "__dt_lsn", "__dt_tx_position", "__dt_commit_time", "__dt_tx_id"} {
			require.False(t, colsByName[name].PrimaryKey)
		}

		var data []map[string]interface{}
		require.NoError(t, st.LoadTable(context.Background(), abstract.TableDescription{
			Name:   tableName,
			Schema: "",
		}, func(input []abstract.ChangeItem) error {
			for _, row := range input {
				if row.Kind == abstract.InsertKind {
					data = append(data, row.AsMap())
				}
			}
			abstract.Dump(input)
			return nil
		}))
		require.Len(t, data, 1)
		row := data[0]
		require.Equal(t, int64(1), asInt64(t, row["id"]))
		require.Equal(t, "test", row["val"])
		// YDB snapshot rows carry zero id/lsn/tx position and an empty tx id.
		require.Equal(t, int64(0), asInt64(t, row["__dt_id"]))
		require.Equal(t, int64(0), asInt64(t, row["__dt_lsn"]))
		require.Equal(t, int64(0), asInt64(t, row["__dt_tx_position"]))
		require.Greater(t, asInt64(t, row["__dt_commit_time"]), int64(0))
		_, hasTxID := row["__dt_tx_id"]
		require.True(t, hasTxID)
		require.Empty(t, row["__dt_tx_id"])
	})
}

func asInt64(t *testing.T, v interface{}) int64 {
	t.Helper()
	switch n := v.(type) {
	case int64:
		return n
	case uint64:
		return int64(n)
	case int:
		return int64(n)
	default:
		t.Fatalf("unexpected numeric type %T (%v)", v, v)
		return 0
	}
}
