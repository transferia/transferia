package logger

import (
	"context"
	"path/filepath"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	ya_zap "go.ytsaurus.tech/library/go/core/log/zap"
)

func TestWrappersPreserveCaller(t *testing.T) {
	core, observed := observer.New(zap.DebugLevel)
	testLogger := NewYtLogBundle(
		ya_zap.NewWithCore(core, zap.AddCaller()),
		ya_zap.NewWithCore(core, zap.AddCaller()),
	)
	previousLogger := Log
	Log = testLogger
	defer func() { Log = previousLogger }()

	ctx := context.Background()
	Info(ctx, "default")
	Infof(ctx, "default formatted %s", "message")
	InfoWithLogger(ctx, testLogger, "custom")
	InfofWithLogger(ctx, testLogger, "custom formatted %s", "message")
	ctxlog.Info(ctx, testLogger, "direct ctxlog")

	entries := observed.All()
	if len(entries) != 5 {
		t.Fatalf("expected five log entries, got %d", len(entries))
	}
	for _, entry := range entries {
		if caller := filepath.Base(entry.Caller.File); caller != "log_test.go" {
			t.Errorf("expected original caller, got %q", entry.Caller.File)
		}
	}
}
