package replication

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/chrecipe"
	provider_sample "github.com/transferia/transferia/pkg/providers/sample"
	"github.com/transferia/transferia/tests/helpers"
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
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "CH target", Port: Target.NativePort},
		))
	}()
	Target.WithDefaults()
	Target.Cleanup = model.DisabledCleanup

	Source.WithDefaults()
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType)

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)

	helpers.Activate(t, transfer)

	helpers.CheckRowsCount(t, &Target, schemaName, "iot", expectedNumberOfRows)
}
