package replacefkeypertable

import (
	"testing"

	"github.com/stretchr/testify/require"
	replace_fkey_common "github.com/transferia/transferia/tests/e2e/mysql2mysql/replace_fkey/common"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/mysql"
)

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "Mysql source", Port: replace_fkey_common.Source.Port},
			network.LabeledPort{Label: "Mysql target", Port: replace_fkey_common.Target.Port},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Existence", replace_fkey_common.Existence)
		t.Run("Snapshot", replace_fkey_common.Snapshot)
		t.Run("Replication", replace_fkey_common.Load)
	})
}
