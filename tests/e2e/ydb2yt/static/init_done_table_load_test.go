package static

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/yt_client"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/testenv"
	"github.com/transferia/transferia/tests/helpers/transfer"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
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
		Path:          "//home/cdc/test/pg2yt_e2e_static_snapshot",
		Cluster:       os.Getenv("YT_PROXY"),
		CellBundle:    "default",
		PrimaryMedium: "default",
		Static:        true,
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

	// init data
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

	// activate transfer
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, src, dst, abstract.TransferTypeSnapshotOnly)
	transfer.TypeSystemVersion = 9
	delivery.Activate(t, transfer)

	// check data

	// To run test locally set YT_PROXY and YT_TOKEN
	config := new(yt.Config)
	client, err := yt_client.NewYtClientWrapper(yt_client.HTTP, nil, config)
	require.NoError(t, err)

	reader, err := client.ReadTable(context.Background(), ypath.Path(dst.Path()).Child("_foo/inserts_delete_test"), nil)
	require.NoError(t, err)

	var data []map[string]interface{}
	for reader.Next() {
		var row map[string]interface{}
		err := reader.Scan(&row)
		require.NoError(t, err)
		data = append(data, row)
	}
	require.Equal(t, data, []map[string]interface{}{
		{"id": int64(1), "val": map[string]interface{}{"a": int64(123)}},
	})
}
