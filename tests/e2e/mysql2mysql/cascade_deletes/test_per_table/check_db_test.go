package cascadedeletespertbl

import (
	"testing"

	"github.com/stretchr/testify/require"
	cascade_deletes_common "github.com/transferia/transferia/tests/e2e/mysql2mysql/cascade_deletes/common"
	"github.com/transferia/transferia/tests/helpers/network"
)

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "Mysql source", Port: cascade_deletes_common.Source.Port},
			network.LabeledPort{Label: "Mysql target", Port: cascade_deletes_common.Target.Port},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Existence", cascade_deletes_common.Existence)
		t.Run("Snapshot", cascade_deletes_common.Snapshot)
		t.Run("Replication", cascade_deletes_common.Load)
	})
}
