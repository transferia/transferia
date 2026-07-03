package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func StartPostgres(t testing.TB, ctx context.Context, additionalOpts ...testcontainers.ContainerCustomizer) *PostgresContainer {
	t.Helper()

	opts := []testcontainers.ContainerCustomizer{
		testcontainers.WithLogger(testcontainers.TestLogger(t)),
	}
	opts = append(opts, additionalOpts...)

	pgc, err := Prepare(ctx, opts...)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, pgc.Terminate(cleanupCtx))
	})

	t.Logf("PostgreSQL is listening at port %d", pgc.Port())

	return pgc
}
