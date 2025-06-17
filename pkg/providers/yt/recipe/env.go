//go:build !disable_yt_provider

package recipe

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"go.ytsaurus.tech/yt/go/mapreduce"
	"go.ytsaurus.tech/yt/go/yttest"
)

func NewEnv(t *testing.T, opts ...yttest.Option) (*yttest.Env, func()) {
	if !TestContainerEnabled() || os.Getenv("YT_PROXY") != "" {
		return yttest.NewEnv(t, opts...)
	}
	ctx := context.Background()
	container, err := RunContainer(ctx, testcontainers.WithImage("ytsaurus/local:stable"))
	require.NoError(t, err)
	f := func() {
		require.NoError(t, container.Terminate(ctx))
	}
	proxy, err := container.ConnectionHost(ctx)
	require.NoError(t, err)
	t.Setenv("YT_PROXY", proxy)
	ytClient, err := container.NewClient(ctx)
	require.NoError(t, err)
	logger, stopLogger := yttest.NewLogger(t)
	ff := func() {
		f()
		stopLogger()
	}
	return &yttest.Env{
		Ctx: ctx,
		YT:  ytClient,
		MR:  mapreduce.New(ytClient),
		L:   logger,
	}, ff
}
