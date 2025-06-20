//go:build !disable_yt_provider

package yt

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/ypath"
)

func TestSafeChildName(t *testing.T) {
	basePath := ypath.Path("//home/cdc/test/kry127")

	require.Equal(t,
		ypath.Path("//home/cdc/test/kry127"),
		SafeChild(basePath, ""),
	)
	require.Equal(t,
		ypath.Path("//home/cdc/test/kry127/basic/usage"),
		SafeChild(basePath, "basic/usage"),
	)
	require.Equal(t,
		ypath.Path("//home/cdc/test/kry127/My/Ydb/Table/Full/Path"),
		SafeChild(basePath, "/My/Ydb/Table/Full/Path"),
		"there should be one slash between correct ypath and appended table name",
	)
	require.Equal(t,
		ypath.Path("//home/cdc/test/kry127/weird/end/slash"),
		SafeChild(basePath, "weird/end/slash/"),
		"after appending table name with ending slash, it should be deleted to form correct ypath",
	)
	require.Equal(t,
		ypath.Path("//home/cdc/test/kry127/Weird/Slashes/Around"),
		SafeChild(basePath, "/Weird/Slashes/Around/"),
		"no slash doubling or ending should occur while appending table name with both beginning and ending slashes",
	)
	require.Equal(t,
		ypath.Path("//home/cdc/test/kry127/Middle/Slashes"),
		SafeChild(basePath, "Middle/////Slashes"),
		"slashes should be deduplicated",
	)
	require.Equal(t,
		ypath.Path("//home/cdc/test/kry127/Append/Multiple/Children/As/Relative/Path"),
		SafeChild(basePath, "Append///Multiple", "///Children///", "As", "/Relative/Path///"),
		"slashes should be deduplicated even when multiple children are appended",
	)
}
