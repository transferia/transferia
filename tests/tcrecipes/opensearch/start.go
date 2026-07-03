package opensearch

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func StartOpenSearch(t testing.TB, ctx context.Context, additionalOpts ...testcontainers.ContainerCustomizer) *OpenSearchContainer {
	t.Helper()

	opts := []testcontainers.ContainerCustomizer{
		testcontainers.WithLogger(testcontainers.TestLogger(t)),
	}
	opts = append(opts, additionalOpts...)

	osc, err := Prepare(ctx, opts...)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, osc.Terminate(cleanupCtx))
	})

	t.Logf("OpenSearch is listening at port %d", osc.HTTPPort())

	return osc
}
