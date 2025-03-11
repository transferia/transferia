package replication

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	chrecipe "github.com/transferia/transferia/pkg/providers/clickhouse/recipe"
	"github.com/transferia/transferia/pkg/providers/sample"
	"github.com/transferia/transferia/tests/helpers"
)

const expectedNumberOfRows = 100

var (
	schemaName   = "mtmobproxy"
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = *sample.RecipeSource()
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
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType)

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)

	helpers.Activate(t, transfer)

	helpers.CheckRowsCount(t, &Target, schemaName, "iot", expectedNumberOfRows)
}
