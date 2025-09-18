package dockercompose

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
)

// StartMariaDBForSource spins up a MariaDB container for the given MysqlSource using testcontainers.
// It mutates the provided source with host/port/user/password/database suitable for local container.
// Returns a cleanup function that terminates the container.
func StartMariaDBForSource(t *testing.T, src *provider_mysql.MysqlSource) func() {
	t.Helper()

	const (
		user     = "test"
		password = "123"
		database = "test"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	req := tc.ContainerRequest{
		Image:        "mariadb:10.6",
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MARIADB_ROOT_PASSWORD": password,
			"MARIADB_DATABASE":      database,
			"MARIADB_USER":          user,
			"MARIADB_PASSWORD":      password,
		},
		Cmd: []string{
			"mysqld",
			"--server-id=1001",
			"--log-bin",
			"--binlog-format=ROW",
			"--gtid-strict-mode=ON",
		},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("3306/tcp").WithStartupTimeout(2 * time.Minute),
		),
	}

	container, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)

	host, err := container.Host(ctx)
	require.NoError(t, err)
	mapped, err := container.MappedPort(ctx, "3306/tcp")
	require.NoError(t, err)

	// Fill source connection params for caller
	src.Host = host
	src.Port = mapped.Int()
	// use root for required privileges (SHOW MASTER STATUS etc.)
	src.User = "root"
	src.Password = password
	src.Database = database

	// Probe with a real connection to be extra safe
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		connParams, err := provider_mysql.NewConnectionParams(src.ToStorageParams())
		if err == nil {
			if db, connErr := provider_mysql.Connect(connParams, nil); connErr == nil {
				_ = db.Close()
				break
			}
		}
		time.Sleep(2 * time.Second)
	}

	return func() {
		// use a fresh context for termination
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_ = container.Terminate(cctx)
	}
}
