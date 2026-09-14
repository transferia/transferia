package replication

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/clickhouse/chrecipe"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	provider_kinesis "github.com/transferia/transferia/pkg/providers/kinesis"
	"github.com/transferia/transferia/pkg/runtime/local"
	canon_reference "github.com/transferia/transferia/tests/canon/reference"
	"github.com/transferia/transferia/tests/helpers"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	"github.com/transferia/transferia/tests/tcrecipes"
)

func init() {

}

func TestReplication(t *testing.T) {
	if !tcrecipes.Enabled() {
		t.Skip()
	}

	var (
		databaseName = "public"
		transferType = abstract.TransferTypeIncrementOnly
		source       = provider_kinesis.MustSource()
		target       = chrecipe.MustTarget(
			chrecipe.WithInitDir("dump/ch"),
			chrecipe.WithDatabase(databaseName))
	)

	transferhelpers.InitSrcDst(transferhelpers.TransferID, source, target, transferType)

	defer func() {
		p := source.Endpoint[len(source.Endpoint)-4:]
		port, err := strconv.Atoi(p)
		require.NoError(t, err)

		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{
				Label: "Kinesis source",
				Port:  port,
			},
			helpers.LabeledPort{
				Label: "CH target Native",
				Port:  target.NativePort,
			},
		))
	}()

	transfer := transferhelpers.MakeTransfer(
		transferhelpers.TransferID,
		source,
		target,
		transferType,
	)

	c := coordinator.NewStatefulFakeClient()
	localWorker := local.NewLocalWorker(
		c,
		transfer,
		helpers.EmptyRegistry(),
		logger.Log,
	)
	localWorker.Start()
	defer localWorker.Stop()

	require.NoError(t, provider_kinesis.PutRecord(
		source,
		[]byte("Hello World!"),
		"test",
	))
	require.NoError(t, provider_kinesis.PutRecord(
		source,
		[]byte("This is a Test"),
		"test",
	))
	require.NoError(t, provider_kinesis.PutRecord(
		source,
		[]byte("testing the test!"),
		"test",
	))

	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		databaseName,
		source.Stream,
		helpers.GetSampleableStorageByModel(t, target),
		60*time.Second,
		3,
	))
	canon_reference.Dump(t, &clickhouse_model.ChSource{
		Database: "public",
		ShardsList: []clickhouse_model.ClickHouseShard{
			{
				Name:  "_",
				Hosts: []string{"localhost"},
			},
		},
		NativePort: target.NativePort,
		HTTPPort:   target.HTTPPort,
		User:       target.User,
	})
}
