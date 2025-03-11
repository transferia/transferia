package downtime

import (
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/providers/clickhouse"
	chrecipe "github.com/transferia/transferia/pkg/providers/clickhouse/recipe"
	"github.com/transferia/transferia/pkg/stats"
	"go.uber.org/zap/zaptest"
	"go.ytsaurus.tech/library/go/core/log/zap"
)

type TestParams struct {
	Host  string
	Alive bool
}

func test(t *testing.T, params TestParams) {
	config := chrecipe.MustTarget(chrecipe.WithInitFile("scripts/init.sql"), chrecipe.WithDatabase("downtime_test"))
	onPingCalled := false

	storageStats := stats.NewChStats(solomon.NewRegistry(nil))

	sinkServer, err := clickhouse.NewSinkServerImplWithVersion(
		config.ToReplicationFromPGSinkParams().MakeChildServerParams(params.Host),
		&zap.Logger{L: zaptest.NewLogger(t)},
		storageStats,
		nil,
		semver.Version{},
	)
	require.NoError(t, err)

	sinkServerEvents := &clickhouse.SinkServerCallbacks{}
	sinkServerEvents.OnPing = func(sinkServer *clickhouse.SinkServer) {
		require.Equal(t, params.Alive, sinkServer.Alive())
		onPingCalled = true
	}
	sinkServer.TestSetCallbackOnPing(sinkServerEvents)
	sinkServer.RunGoroutines()

	if sinkServer.Alive() {
		require.Equal(t, params.Alive, sinkServer.Alive())
	}

	time.Sleep(1 * time.Second)

	require.True(t, onPingCalled)
}

func TestAliveNoZK(t *testing.T) {
	test(t, TestParams{Host: "localhost", Alive: true})
}

func TestDowntime(t *testing.T) {
	// this test is pointless, but I don't want to delete it.
	test(t, TestParams{Host: "fake", Alive: false})
}
