package replacefkeypertable

import (
	"testing"

	"github.com/stretchr/testify/require"
	test "github.com/transferria/transferria/tests/e2e/mysql2mysql/replace_fkey/common"
	"github.com/transferria/transferria/tests/helpers"
)

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: test.Source.Port},
			helpers.LabeledPort{Label: "Mysql target", Port: test.Target.Port},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Existence", test.Existence)
		t.Run("Snapshot", test.Snapshot)
		t.Run("Replication", test.Load)
	})
}
