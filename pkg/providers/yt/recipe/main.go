//go:build !disable_yt_provider

package recipe

import (
	"context"
	"os"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	ytcommon "github.com/transferia/transferia/pkg/providers/yt"
)

func Main(m *testing.M) {
	ctx, cancel := context.WithCancel(context.Background())
	container, _ := RunContainer(ctx, testcontainers.WithImage("ytsaurus/local:stable"))
	proxy, _ := container.ConnectionHost(ctx)
	_ = os.Setenv("YT_PROXY", proxy)
	ytcommon.InitExe()
	res := m.Run()
	_ = container.Terminate(ctx)
	cancel()
	os.Exit(res)
}
