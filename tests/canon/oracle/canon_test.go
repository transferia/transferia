package oracle

import (
	"context"
	_ "embed"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/oracle/oraclerecipe"
	"github.com/transferia/transferia/tests/canon/validator"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/oracle"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

//go:embed dump/init.sql
var initSQL string

// TestCanonSource snapshots the Oracle type groups from dump/init.sql.
// Those extracted rows are what canon.All returns for an Oracle source.
func TestCanonSource(t *testing.T) {
	t.Setenv("YC", "1") // to not go to vanga
	source := oraclerecipe.RecipeOracleSource()
	require.NoError(t, oraclerecipe.ExecSQL(context.Background(), source, initSQL))
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "Oracle source", Port: source.Port},
		))
	}()

	tables := []struct {
		name    string
		include string
	}{
		{name: "all_types", include: "DT_TEST.ALL_TYPES"},
		{name: "nchar_types", include: "DT_TEST.NCHAR_TYPES"},
		{name: "tslocal_types", include: "DT_TEST.TSLOCAL_TYPES"},
		{name: "nclob_types", include: "DT_TEST.NCLOB_TYPES"},
		{name: "blob_types", include: "DT_TEST.BLOB_TYPES"},
		{name: "long_text", include: "DT_TEST.LONG_TEXT"},
		{name: "long_binary", include: "DT_TEST.LONG_BINARY"},
	}
	for _, table := range tables {
		table := table
		t.Run(table.name, func(t *testing.T) {
			src := *source
			src.IncludeTables = []string{table.include}
			transfer := transferhelpers.MakeTransfer(
				table.name,
				&src,
				&model.MockDestination{
					SinkerFactory: validator.New(
						model.IsStrictSource(&src),
						validator.InitDone(t),
						validator.ValuesTypeChecker,
						validator.CanonizatorSkipEmptyClose(t),
					),
					Cleanup: model.DisabledCleanup,
				},
				abstract.TransferTypeSnapshotOnly,
			)
			_ = delivery.Activate(t, transfer)
		})
	}
}
