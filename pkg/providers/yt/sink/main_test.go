//go:build !disable_yt_provider

package sink

import (
	"os"
	"testing"

	"github.com/transferia/transferia/pkg/config/env"
	ytcommon "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/recipe"
)

func TestMain(m *testing.M) {
	if recipe.TestContainerEnabled() {
		recipe.Main(m)
		return
	}
	if env.IsTest() && !recipe.TestContainerEnabled() {
		ytcommon.InitExe()
	}
	os.Exit(m.Run())
}
