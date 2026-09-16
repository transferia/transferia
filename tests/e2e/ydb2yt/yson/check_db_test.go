package snapshot

import (
	"context"
	"fmt"
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
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/testenv"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestGroup(t *testing.T) {
	src := &provider_ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           testenv.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           testenv.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             nil,
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
	}
	dst := provider_yt.NewYtDestinationV1(provider_yt.YtDestination{
		Path:                     "//home/cdc/test/pg2yt_e2e",
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
		Target := &provider_ydb.YdbDestination{
			Database: src.Database,
			Token:    src.Token,
			Instance: src.Instance,
		}
		Target.WithDefaults()
		sinker, err := provider_ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)
		testSchema := abstract.NewTableSchema([]abstract.ColSchema{
			{ColumnName: "id", DataType: string(ytschema.TypeInt32), PrimaryKey: true},
			{ColumnName: "val", DataType: string(ytschema.TypeAny), OriginalType: "ydb:Yson"},
		})
		require.NoError(t, sinker.Push([]abstract.ChangeItem{{
			Kind:         abstract.InsertKind,
			Schema:       "",
			Table:        "foo/inserts_delete_test",
			ColumnNames:  []string{"id", "val"},
			ColumnValues: []interface{}{1, map[string]interface{}{"a": 123}},
			TableSchema:  testSchema,
		}}))
	})

	t.Run("activate transfer", func(t *testing.T) {
		transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, src, dst, abstract.TransferTypeSnapshotOnly)
		require.NoError(t, tasks.ActivateDelivery(context.TODO(), nil, coordinator.NewStatefulFakeClient(), *transfer, testmetrics.EmptyRegistry()))
	})

	t.Run("check data", func(t *testing.T) {
		ytStorageParams := provider_yt.YtStorageParams{
			Token:   dst.Token(),
			Cluster: os.Getenv("YT_PROXY"),
			Path:    dst.Path(),
			Spec:    nil,
		}
		st, err := yt_storage.NewStorage(&ytStorageParams)
		require.NoError(t, err)
		var data []map[string]interface{}
		require.NoError(t, st.LoadTable(context.Background(), abstract.TableDescription{
			Name:   "foo/inserts_delete_test",
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
		fmt.Printf("data %v \n", data)
		require.Equal(t, data, []map[string]interface{}{
			{"id": int64(1), "val": map[string]interface{}{"a": int64(123)}},
		})
	})
}
