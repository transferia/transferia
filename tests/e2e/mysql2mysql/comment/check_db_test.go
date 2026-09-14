package comment_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	"github.com/transferia/transferia/tests/helpers"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = *helpers.RecipeMysqlSource()
	Target       = *helpers.RecipeMysqlTarget(mysqlrecipe.WithPrefix("TARGET_"))
)

func init() {
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: Source.Port},
			helpers.LabeledPort{Label: "Mysql target", Port: Target.Port},
		))
	}()

	t.Run("Main group", func(t *testing.T) {
		t.Run("Existence", Existence)
		t.Run("Snapshot", Snapshot)
	})
}

func Existence(t *testing.T) {
	_, err := provider_mysql.NewStorage(Source.ToStorageParams())
	require.NoError(t, err)
	_, err = provider_mysql.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
}

func Snapshot(t *testing.T) {
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)
	_ = helpers.Activate(t, transfer)
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
