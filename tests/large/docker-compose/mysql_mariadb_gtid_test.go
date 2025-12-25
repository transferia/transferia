package dockercompose

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/tests/helpers"
)

// This test ensures that for MariaDB flavor we read GTID via @@GLOBAL.gtid_current_pos
// and propagate it into LogPosition.TxID when creating a snapshot position.
func TestMySQL_MariaDB_GtidPosition(t *testing.T) {
	t.Parallel()

	// Prepare source and start container
	src := provider_mysql.MysqlSource{
		IncludeTableRegex:  []string{"test.t"},
		Database:           "test",
		ConsistentSnapshot: true,
	}
	cleanup := StartMariaDBForSource(t, &src)
	defer cleanup()

	// Ensure binlog+gtid is usable by doing a write
	connParams := helpers.NewMySQLConnectionParams(t, src.ToStorageParams())
	helpers.ExecuteMySQLStatement(t, "CREATE TABLE IF NOT EXISTS t(id INT PRIMARY KEY)", connParams)
	helpers.ExecuteMySQLStatement(t, "INSERT INTO t(id) VALUES (1) ON DUPLICATE KEY UPDATE id = id", connParams)

	// Create storage and read position
	storage := helpers.NewMySQLStorageFromSource(t, &src)
	defer storage.Close()

	pos, err := storage.Position(context.Background())
	require.NoError(t, err)
	require.NotNil(t, pos)
	t.Logf("pos: %+v", pos)
	require.NotZero(t, pos.ID)
	require.NotEmpty(t, pos.TxID, "TxID (GTID set) should be non-empty for MariaDB")
}
