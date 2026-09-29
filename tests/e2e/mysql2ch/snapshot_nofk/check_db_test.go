package snapshotnofk

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse/chrecipe"
	"github.com/transferia/transferia/tests/e2e/mysql2ch"
	"github.com/transferia/transferia/tests/e2e/pg2ch"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/mysql"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/clickhouse"
	_ "github.com/transferia/transferia/tests/helpers/registration/mysql"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

func TestSnapshot(t *testing.T) {
	source := mysql.RecipeMysqlSource()
	target := chrecipe.MustTarget(chrecipe.WithInitFile("ch.sql"), chrecipe.WithDatabase("source"))

	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "MySQL source", Port: source.Port},
			network.LabeledPort{Label: "CH target", Port: target.NativePort},
		))
	}()

	t.Run("fake_keys", func(t *testing.T) {
		source.UseFakePrimaryKey = true
		transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, source, target, abstract.TransferTypeSnapshotAndIncrement)
		_, err := delivery.ActivateErr(transfer)
		require.NoError(t, err)
		require.NoError(t, storagecomparison.CompareStorages(
			t,
			source,
			target,
			storagecomparison.NewCompareStorageParams().WithEqualDataTypes(pg2ch.PG2CHDataTypesComparator).WithPriorityComparators(mysql2ch.MySQLBytesToStringOptionalComparator),
		))
	})
	t.Run("no_fake_keys", func(t *testing.T) {
		source.UseFakePrimaryKey = false
		transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, source, target, abstract.TransferTypeSnapshotAndIncrement)
		_, err := delivery.ActivateErr(transfer)
		require.Error(t, err)
	})
}
