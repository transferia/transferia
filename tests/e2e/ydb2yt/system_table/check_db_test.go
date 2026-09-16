package main

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	yt_storage "github.com/transferia/transferia/pkg/providers/yt/storage"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/testenv"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

func TestGroup(t *testing.T) {
	src := &provider_ydb.YdbSource{
		Token:              "",
		Database:           testenv.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           testenv.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             []string{".sys/ds_groups"},
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           true,
		ServiceAccountID:   "",
	}
	dst := provider_yt.NewYtDestinationV1(provider_yt.YtDestination{
		Path:                     "//home/cdc/test/pg2yt_e2e",
		Cluster:                  os.Getenv("YT_PROXY"),
		CellBundle:               "default",
		PrimaryMedium:            "default",
		UseStaticTableOnSnapshot: true,
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
		var rows []abstract.ChangeItem
		require.NoError(t, st.LoadTable(context.Background(), abstract.TableDescription{
			Name:   "ds_groups",
			Schema: "",
		}, func(input []abstract.ChangeItem) error {
			for _, row := range input {
				if row.Kind == abstract.InsertKind {
					rows = append(rows, row)
				}
			}
			abstract.Dump(input)
			return nil
		}))
		fmt.Printf("data %v \n", rows)
		require.Equal(t, 3, len(rows))
	})
}
