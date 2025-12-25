package replication

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	cpclient "github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	chrecipe "github.com/transferia/transferia/pkg/providers/clickhouse/recipe"
	"github.com/transferia/transferia/pkg/providers/kinesis"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/tests/canon/reference"
	"github.com/transferia/transferia/tests/helpers"
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
		source       = kinesis.MustSource()
		target       = chrecipe.MustTarget(
			chrecipe.WithInitDir("dump/ch"),
			chrecipe.WithDatabase(databaseName))
	)

	helpers.InitSrcDst(helpers.TransferID, source, target, transferType)

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

	transfer := helpers.MakeTransfer(
		helpers.TransferID,
		source,
		target,
		transferType,
	)

	c := cpclient.NewStatefulFakeClient()
	localWorker := local.NewLocalWorker(
		c,
		transfer,
		helpers.EmptyRegistry(),
		logger.Log,
	)
	localWorker.Start()
	defer localWorker.Stop()

	require.NoError(t, kinesis.PutRecord(
		source,
		[]byte("Hello World!"),
		"test",
	))
	require.NoError(t, kinesis.PutRecord(
		source,
		[]byte("This is a Test"),
		"test",
	))
	require.NoError(t, kinesis.PutRecord(
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
	reference.Dump(t, &model.ChSource{
		Database: "public",
		ShardsList: []model.ClickHouseShard{
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
