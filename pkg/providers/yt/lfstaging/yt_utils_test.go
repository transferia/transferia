//go:build !disable_yt_provider

package lfstaging

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/yt/recipe"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

func TestListNodes(t *testing.T) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()

	dirPath := ypath.Path("//yt-utils-test")

	_, err := env.YT.CreateNode(env.Ctx, dirPath, yt.NodeMap, &yt.CreateNodeOptions{})
	require.NoError(t, err, "CreateNode throws")

	_, err = env.YT.CreateNode(env.Ctx, dirPath.Child("one"), yt.NodeTable, &yt.CreateNodeOptions{})
	require.NoError(t, err, "CreateNode throws")

	_, err = env.YT.CreateNode(env.Ctx, dirPath.Child("two"), yt.NodeTable, &yt.CreateNodeOptions{})
	require.NoError(t, err, "CreateNode throws")

	nodes, err := listNodes(env.YT, dirPath)
	require.NoError(t, err, "listNodes throws")

	require.Equal(t, len(nodes), 2)
	require.Equal(t, nodes[0].Path, "//yt-utils-test/two")
	require.Equal(t, nodes[1].Path, "//yt-utils-test/one")
}

func TestListLockedNodes(t *testing.T) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()

	config := defaultSinkConfig()
	config.tmpPath = "//yt-utils-test/list-locked-nodes"

	_, err := newIntermediateWriter(config, env.YT, env.L.Logger())
	require.NoError(t, err, "newIntermediateWriter throws")

	// Intermediate writer should have rotated the table on startup, so we should be able to see the lock.

	nodes, err := listNodes(env.YT, config.tmpPath)
	require.NoError(t, err, "listNode throws")

	require.Equal(t, 1, len(nodes))
	require.False(t, nodes[0].IsWriterFinished)
}

func TestListUnlockedNodes(t *testing.T) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()

	config := defaultSinkConfig()
	config.tmpPath = "//yt-utils-test/list-unlocked-nodes"

	iw, err := newIntermediateWriter(config, env.YT, env.L.Logger())
	require.NoError(t, err, "newIntermediateWriter throws")
	err = iw.Write([]abstract.ChangeItem{
		abstract.MakeRawMessage(
			"fake-topic",
			time.Now(),
			"fake-topic",
			0,
			0,
			[]byte{},
		),
	})
	require.NoError(t, err, "iw.Write() throws")

	nodesBeforeRotate, err := listNodes(env.YT, config.tmpPath)
	require.NoError(t, err, "listNodes throws")

	require.NoError(t, iw.rotate(), "iw.rotate() throws")

	// Rotating the table once should leave us with one table with IsWriterFinished=false and another with IsWriterFinished=true.

	nodes, err := listNodes(env.YT, config.tmpPath)
	require.NoError(t, err, "listNodes throws")

	require.Equal(t, 2, len(nodes))

	lockedAmount := 0
	unlockedAmount := 0
	for _, node := range nodes {
		if node.Type != "table" {
			continue
		}
		if node.IsWriterFinished {
			unlockedAmount++
			require.Equal(t, nodesBeforeRotate[0].Name, node.Name)
		} else {
			lockedAmount++
		}
	}

	require.Equal(t, 1, lockedAmount)
	require.Equal(t, 1, unlockedAmount)
}
