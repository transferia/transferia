package yt

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	yt_provider "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/recipe"
	"go.ytsaurus.tech/yt/go/ypath"
	commonyt "go.ytsaurus.tech/yt/go/yt"
)

func TestListNodesWithAttrs(t *testing.T) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()
	client := env.YT
	defer client.Stop()
	var err error

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err = client.CreateNode(ctx, ypath.Path("//home/cdc/test"), commonyt.NodeMap, &commonyt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)

	_, err = client.CreateNode(ctx, ypath.Path("//home/cdc/test/node1"), commonyt.NodeTable, &commonyt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)

	_, err = client.CreateNode(ctx, ypath.Path("//home/cdc/test/node2"), commonyt.NodeTable, &commonyt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)

	_, err = client.CreateNode(ctx, ypath.Path("//home/cdc/test1/node1"), commonyt.NodeTable, &commonyt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)

	_, err = client.CreateNode(ctx, ypath.Path("//home/cdc/test1/node2"), commonyt.NodeTable, &commonyt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)

	nodes, err := yt_provider.ListNodesWithAttrs(ctx, client, ypath.Path("//home/cdc"), "test/node", true)
	require.NoError(t, err)
	require.Equal(t, len(nodes), 2)
	require.Equal(t, nodes[0].Name, "test/node1")
}
