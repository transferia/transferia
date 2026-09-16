package replication

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/chrecipe"
	provider_sample "github.com/transferia/transferia/pkg/providers/sample"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

const expectedNumberOfRows = 100

var (
	schemaName   = "mtmobproxy"
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = *provider_sample.RecipeSource()
	Target       = *chrecipe.MustTarget(chrecipe.WithInitFile("dump/dst.sql"), chrecipe.WithDatabase(schemaName), chrecipe.WithPrefix("DB0_"))
)

func TestSnapshot(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "CH target", Port: Target.NativePort},
		))
	}()
	Target.WithDefaults()
	Target.Cleanup = model.DisabledCleanup

	Source.WithDefaults()
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType)

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)

	delivery.Activate(t, transfer)

	storagecomparison.CheckRowsCount(t, &Target, schemaName, "iot", expectedNumberOfRows)
}
