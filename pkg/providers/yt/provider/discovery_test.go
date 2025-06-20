//go:build !disable_yt_provider

package provider

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/base/filter"
	yt_provider "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/recipe"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

func buildSchema(schema []yt_provider.ColumnSchema) []map[string]string {
	res := make([]map[string]string, len(schema))
	for idx, col := range schema {
		res[idx] = map[string]string{
			"name": col.Name,
			"type": string(col.YTType),
		}
	}

	return res
}

func TestTablesDiscovery(t *testing.T) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()

	ctx := context.Background()

	rootPath := ypath.Path("//home/cdc/junk/TestTablesDiscovery")
	_, err := env.YT.CreateNode(ctx, rootPath, yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)
	defer func() {
		err := env.YT.RemoveNode(ctx, rootPath, &yt.RemoveNodeOptions{Recursive: true})
		require.NoError(t, err)
	}()

	require.NoError(t, createTestTable(env, ctx, rootPath.Child("sample_table_1")))
	require.NoError(t, createTestTable(env, ctx, rootPath.Child("sample_table_2")))
	require.NoError(t, createTestTable(env, ctx, rootPath.Child("sample_table_3")))
	require.NoError(t, createTestTable(env, ctx, rootPath.Child("sample_table_4")))
	require.NoError(t, createTestTable(env, ctx, rootPath.Child("sample_table_5")))
	_, err = env.YT.CreateNode(ctx, rootPath.Child("some_dir"), yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)
	require.NoError(t, createTestTable(env, ctx, rootPath.Child("some_dir").Child("sample_table_1")))
	require.NoError(t, createTestTable(env, ctx, rootPath.Child("some_dir").Child("sample_table_2")))
	_, err = env.YT.CreateNode(ctx, rootPath.Child("some_dir").Child("sample_non_table_obj"), yt.NodeFile, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)
	t.Run("all_tables", func(t *testing.T) {
		cfg := &yt_provider.YtSource{
			Cluster: os.Getenv("YT_PROXY"),
			Proxy:   os.Getenv("YT_PROXY"),
			Paths:   []string{rootPath.String()},
			YtToken: os.Getenv("YT_TOKEN"),
		}

		src, err := NewSource(logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), cfg)
		require.NoError(t, err)

		require.NoError(t, src.Init())
		require.NoError(t, src.BeginSnapshot())

		objs, err := src.DataObjects(nil)
		require.NoError(t, err)
		objNames, err := listObjects(objs)
		require.NoError(t, err)
		require.Len(t, objNames, 7)
	})
	t.Run("2_tables", func(t *testing.T) {
		cfg := &yt_provider.YtSource{
			Cluster: os.Getenv("YT_PROXY"),
			Proxy:   os.Getenv("YT_PROXY"),
			Paths:   []string{rootPath.String()},
			YtToken: os.Getenv("YT_TOKEN"),
		}

		src, err := NewSource(logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), cfg)
		require.NoError(t, err)

		require.NoError(t, src.Init())
		require.NoError(t, src.BeginSnapshot())

		f, err := filter.NewFromObjects([]string{
			rootPath.Child("sample_table_2").String(),
			rootPath.Child("sample_table_5").String(),
		})
		require.NoError(t, err)
		objs, err := src.DataObjects(f)
		require.NoError(t, err)
		objNames, err := listObjects(objs)
		require.NoError(t, err)
		require.Len(t, objNames, 2)
	})
	t.Run("error_for_non_tables", func(t *testing.T) {
		cfg := &yt_provider.YtSource{
			Cluster: os.Getenv("YT_PROXY"),
			Proxy:   os.Getenv("YT_PROXY"),
			Paths:   []string{rootPath.String()},
			YtToken: os.Getenv("YT_TOKEN"),
		}

		src, err := NewSource(logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), cfg)
		require.NoError(t, err)

		require.NoError(t, src.Init())
		require.NoError(t, src.BeginSnapshot())

		f, err := filter.NewFromObjects([]string{
			rootPath.Child("sample_table_2").String(),
			rootPath.Child("sample_table_6").String(),
		})
		require.NoError(t, err)
		objs, err := src.DataObjects(f)
		require.NoError(t, err)
		_, err = listObjects(objs)
		require.Error(t, err)
	})
	t.Run("error_for_none_table_path", func(t *testing.T) {
		cfg := &yt_provider.YtSource{
			Cluster: os.Getenv("YT_PROXY"),
			Proxy:   os.Getenv("YT_PROXY"),
			Paths:   []string{rootPath.String()},
			YtToken: os.Getenv("YT_TOKEN"),
		}

		src, err := NewSource(logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), cfg)
		require.NoError(t, err)

		require.NoError(t, src.Init())
		require.NoError(t, src.BeginSnapshot())

		f, err := filter.NewFromObjects([]string{
			rootPath.Child("sample_table_2").String(),
			rootPath.Child("some_dir").Child("sample_non_table_obj").String(), // exist, but not table
		})
		require.NoError(t, err)
		objs, err := src.DataObjects(f)
		require.NoError(t, err)
		_, err = listObjects(objs)
		require.Error(t, err)
	})
	t.Run("no_error_when_ask_dir", func(t *testing.T) {
		cfg := &yt_provider.YtSource{
			Cluster: os.Getenv("YT_PROXY"),
			Proxy:   os.Getenv("YT_PROXY"),
			Paths:   []string{rootPath.String()},
			YtToken: os.Getenv("YT_TOKEN"),
		}

		src, err := NewSource(logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), cfg)
		require.NoError(t, err)

		require.NoError(t, src.Init())
		require.NoError(t, src.BeginSnapshot())

		f, err := filter.NewFromObjects([]string{
			rootPath.Child("sample_table_2").String(),
			rootPath.Child("some_dir").String(),
		})
		require.NoError(t, err)
		objs, err := src.DataObjects(f)
		require.NoError(t, err)
		objNames, err := listObjects(objs)
		require.NoError(t, err)
		require.Len(t, objNames, 3)
	})
}

func createTestTable(env *yttest.Env, ctx context.Context, tablePath ypath.Path) error {
	_, err := env.YT.CreateNode(ctx, tablePath, yt.NodeTable, &yt.CreateNodeOptions{
		Attributes: map[string]interface{}{
			"schema": buildSchema([]yt_provider.ColumnSchema{
				{
					Name:    "Column_1",
					YTType:  "int8",
					Primary: true,
				},
				{
					Name:    "Column_2",
					YTType:  "int8",
					Primary: false,
				},
			},
			),
		},
	})
	return err
}

func listObjects(objs base.DataObjects) ([]string, error) {
	var res []string
	for objs.Next() {
		obj, err := objs.Object()
		if err != nil {
			return nil, err
		}
		res = append(res, obj.FullName())
	}
	return res, objs.Err()
}
