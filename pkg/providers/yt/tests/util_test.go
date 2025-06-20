//go:build !disable_yt_provider

package yt

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yt_provider "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/recipe"
	"github.com/transferia/transferia/pkg/randutil"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type testObject struct {
	Key   string `yson:"key,key"`
	Value string `yson:"value"`
}

type createNodeTreeParams struct {
	DirCount          int
	DynamicTableCount int
	DynamicTableAttrs map[string]interface{}
	StaticTableCount  int
	StaticTableAttrs  map[string]interface{}
}

func createNodeTree(
	ctx context.Context,
	client yt.Client,
	path ypath.Path,
	level int,
	params *createNodeTreeParams,
	dynamicTables *[]ypath.Path,
	staticTables *[]ypath.Path,
) error {
	if level > 0 {
		for i := 0; i < params.DirCount; i++ {
			dirPath := path.Child(fmt.Sprintf("dir%v", i))
			_, err := client.CreateNode(ctx, dirPath, yt.NodeMap, nil)
			if err != nil {
				return xerrors.Errorf("unable to create directory '%v': %w", dirPath, err)
			}
			err = createNodeTree(ctx, client, dirPath, level-1, params, dynamicTables, staticTables)
			if err != nil {
				return err
			}
		}

		if err := createTables(ctx, client, path, "dynamic_table", params.DynamicTableCount, params.DynamicTableAttrs, dynamicTables); err != nil {
			return xerrors.Errorf("unable to create dynamic tables: %w", err)
		}

		if err := createTables(ctx, client, path, "static_table", params.StaticTableCount, params.StaticTableAttrs, staticTables); err != nil {
			return xerrors.Errorf("unable to create dynamic tables: %w", err)
		}
	}

	return nil
}

func createTables(ctx context.Context, client yt.Client, path ypath.Path, name string, count int, attrs map[string]interface{}, tables *[]ypath.Path) error {
	for i := 0; i < count; i++ {
		tablePath := path.Child(fmt.Sprintf("%v%v", name, i))
		_, err := client.CreateNode(ctx, tablePath, yt.NodeTable, &yt.CreateNodeOptions{
			Attributes: attrs,
		})
		if err != nil {
			return xerrors.Errorf("unable to create table '%v': %w", tablePath, err)
		}
		*tables = append(*tables, tablePath)
	}
	return nil
}

func TestMountUnmount(t *testing.T) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()
	client := env.YT
	defer client.Stop()
	var err error

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testDir := randutil.GenerateAlphanumericString(10)

	path := ypath.Path("//home/cdc/test/mount_unmount").Child(testDir)
	logger.Log.Infof("test dir: %v", path)
	_, err = client.CreateNode(ctx, path, yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)

	var dynamicTables []ypath.Path
	var staticTables []ypath.Path
	err = createNodeTree(ctx, client, path, 3, &createNodeTreeParams{
		DirCount:          2,
		DynamicTableCount: 2,
		DynamicTableAttrs: map[string]interface{}{
			"dynamic": true,
			"schema":  schema.MustInfer(new(testObject)),
		},
		StaticTableCount: 2,
		StaticTableAttrs: map[string]interface{}{
			"schema": schema.MustInfer(new(testObject)),
		},
	}, &dynamicTables, &staticTables)
	require.NoError(t, err)

	handleParams := yt_provider.NewHandleParams(5)

	err = yt_provider.MountAndWaitRecursive(ctx, logger.Log, client, path, handleParams)
	require.NoError(t, err)
	for _, table := range dynamicTables {
		attrs := new(yt_provider.NodeAttrs)
		err = client.GetNode(ctx, table.Attrs(), attrs, nil)
		require.NoError(t, err)
		require.Truef(t, attrs.Dynamic, "table '%v' must be dynamic", table)
		require.Equalf(t, yt.TabletMounted, attrs.TabletState, "table '%v' is not mounted", table)
	}

	for _, table := range staticTables {
		attrs := new(yt_provider.NodeAttrs)
		err = client.GetNode(ctx, table.Attrs(), attrs, nil)
		require.NoError(t, err)
		require.Falsef(t, attrs.Dynamic, "table '%v' must be static", table)
	}

	err = yt_provider.UnmountAndWaitRecursive(ctx, logger.Log, client, path, handleParams)
	require.NoError(t, err)
	for _, table := range dynamicTables {
		attrs := new(yt_provider.NodeAttrs)
		err = client.GetNode(ctx, table.Attrs(), attrs, nil)
		require.NoError(t, err)
		require.Truef(t, attrs.Dynamic, "table '%v' must be dynamic", table)
		require.Equalf(t, yt.TabletUnmounted, attrs.TabletState, "table '%v' is not unmounted", table)
	}

	for _, table := range staticTables {
		attrs := new(yt_provider.NodeAttrs)
		err = client.GetNode(ctx, table.Attrs(), attrs, nil)
		require.NoError(t, err)
		require.Falsef(t, attrs.Dynamic, "table '%v' must be static", table)
	}

	err = client.RemoveNode(ctx, path, &yt.RemoveNodeOptions{Recursive: true})
	require.NoError(t, err)
}
