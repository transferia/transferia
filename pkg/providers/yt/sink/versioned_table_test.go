//go:build !disable_yt_provider

package sink

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/internal/metrics"
	"github.com/transferia/transferia/pkg/abstract"
	client2 "github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/recipe"
	"go.ytsaurus.tech/yt/go/ypath"
	ytsdk "go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var versionedSchema = abstract.NewTableSchema([]abstract.ColSchema{
	{ColumnName: "key", DataType: "string", PrimaryKey: true},
	{ColumnName: "version", DataType: "int32"},
	{ColumnName: "value", DataType: "string"},
})

const (
	testVersionedTablePath = "//home/cdc/test/versioned/test_table"
)

type testVersionedRow struct {
	Key     string `yson:"key"`
	Version int    `yson:"version"`
	Value   string `yson:"value"`
}

type skippedVersionedRow struct {
	Key           string `yson:"key"`
	StoredVersion string `yson:"__stored_version"`
	Version       int    `yson:"version"`
	Value         string `yson:"value"`
}

func TestVersionedTable_Write(t *testing.T) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()
	defer teardown(env.YT, testVersionedTablePath)
	cfg := yt.NewYtDestinationV1(versionTableYtConfig())
	cfg.WithDefaults()
	table, err := newSinker(cfg, "some_uniq_transfer_id", logger.Log, metrics.NewRegistry(), client2.NewFakeClient())
	require.NoError(t, err)
	err = table.Push(generateVersionRows(2, 1))
	require.NoError(t, err)
	err = table.Push(generateVersionRows(2, 2))
	require.NoError(t, err)
	storedRows := readVersionedTableStored(t, env)
	require.Equal(t, 2, len(storedRows))
	for _, r := range storedRows {
		require.Equal(t, 2, r.Version)
	}
}

func TestVersionedTable_Write_Newest_Than_Oldest(t *testing.T) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()
	defer teardown(env.YT, testVersionedTablePath)
	cfg := yt.NewYtDestinationV1(versionTableYtConfig())
	cfg.WithDefaults()
	table, err := newSinker(cfg, "some_uniq_transfer_id", logger.Log, metrics.NewRegistry(), client2.NewFakeClient())
	require.NoError(t, err)
	err = table.Push(generateVersionRows(2, 2))
	require.NoError(t, err)
	err = table.Push(generateVersionRows(2, 1))
	require.NoError(t, err)
	storedRows, skippedRows := readVersionedTable(t, env)
	require.Equal(t, 2, len(storedRows))
	for _, r := range storedRows {
		require.Equal(t, 2, r.Version)
	}
	require.Equal(t, 2, len(skippedRows))
	for _, r := range skippedRows {
		require.Equal(t, 1, r.Version)
		require.Equal(t, "2", r.StoredVersion)
	}
}

func TestVersionedTable_Write_MissedOrder(t *testing.T) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()
	defer teardown(env.YT, testVersionedTablePath)
	cfg := yt.NewYtDestinationV1(versionTableYtConfig())
	cfg.WithDefaults()
	table, err := newSinker(cfg, "some_uniq_transfer_id", logger.Log, metrics.NewRegistry(), client2.NewFakeClient())
	require.NoError(t, err)
	input := append(generateVersionRows(2, 2), generateVersionRows(2, 1)...)
	require.NoError(t, table.Push(input))
	storedRows := readVersionedTableStored(t, env)
	require.Equal(t, 2, len(storedRows))
	for _, r := range storedRows {
		require.Equal(t, 2, r.Version)
	}
}

func TestVersionedTable_CustomAttributes(t *testing.T) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()
	defer teardown(env.YT, testVersionedTablePath)
	cfg := yt.NewYtDestinationV1(versionTableYtConfig())
	cfg.WithDefaults()
	table, err := newSinker(cfg, "some_uniq_transfer_id", logger.Log, metrics.NewRegistry(), client2.NewFakeClient())
	require.NoError(t, err)
	input := append(generateVersionRows(2, 2), generateVersionRows(2, 1)...)
	require.NoError(t, table.Push(input))
	var data bool
	require.NoError(t, env.YT.GetNode(env.Ctx, ypath.Path(fmt.Sprintf("%s/@test", testVersionedTablePath)), &data, nil))
	require.Equal(t, true, data)
}

func TestVersionedTable_IncludeTimeoutAttribute(t *testing.T) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()
	defer teardown(env.YT, testVersionedTablePath)
	cfg := yt.NewYtDestinationV1(versionTableYtConfig())
	cfg.WithDefaults()
	table, err := newSinker(cfg, "some_uniq_transfer_id", logger.Log, metrics.NewRegistry(), client2.NewFakeClient())
	require.NoError(t, err)
	input := append(generateVersionRows(2, 2), generateVersionRows(2, 1)...)
	require.NoError(t, table.Push(input))
	var timeout int64
	require.NoError(t, env.YT.GetNode(env.Ctx, ypath.Path(fmt.Sprintf("%s/@expiration_timeout", testVersionedTablePath)), &timeout, nil))
	require.Equal(t, int64(604800000), timeout)
	var expTime string
	require.NoError(t, env.YT.GetNode(env.Ctx, ypath.Path(fmt.Sprintf("%s/@expiration_time", testVersionedTablePath)), &expTime, nil))
	require.Equal(t, "2200-01-12T03:32:51.298047Z", expTime)
}

func readVersionedTableStored(t *testing.T, env *yttest.Env) []testVersionedRow {
	rows, err := env.YT.SelectRows(
		env.Ctx,
		fmt.Sprintf("* from [%v]", testVersionedTablePath),
		nil,
	)
	require.NoError(t, err)
	var storedRows []testVersionedRow
	for rows.Next() {
		var row testVersionedRow
		require.NoError(t, rows.Scan(&row))
		storedRows = append(storedRows, row)
	}
	return storedRows
}

func readVersionedTableSkipped(t *testing.T, env *yttest.Env) []skippedVersionedRow {
	rows, err := env.YT.SelectRows(
		env.Ctx,
		fmt.Sprintf("* from [%v_skipped]", testVersionedTablePath),
		nil,
	)
	require.NoError(t, err)
	var skippedRows []skippedVersionedRow
	for rows.Next() {
		var row skippedVersionedRow
		require.NoError(t, rows.Scan(&row))
		skippedRows = append(skippedRows, row)
	}
	return skippedRows
}

func readVersionedTable(t *testing.T, env *yttest.Env) ([]testVersionedRow, []skippedVersionedRow) {
	return readVersionedTableStored(t, env), readVersionedTableSkipped(t, env)
}

func generateVersionRows(count, version int) []abstract.ChangeItem {
	res := make([]abstract.ChangeItem, 0)
	for i := 0; i < count; i++ {
		item := abstract.ChangeItem{
			Kind:        "insert",
			ColumnNames: []string{"key", "version", "value"},
			ColumnValues: []interface{}{
				fmt.Sprintf("v-%v", i),
				version,
				fmt.Sprintf("val-%v at version %v", i, version),
			},
			TableSchema: versionedSchema,
			Table:       "test_table",
		}
		res = append(res, item)
	}
	return res
}

func versionTableYtConfig() yt.YtDestination {
	return yt.YtDestination{
		Atomicity:     ytsdk.AtomicityFull,
		VersionColumn: "version",
		OptimizeFor:   "scan",
		CellBundle:    "default",
		PrimaryMedium: "default",
		Path:          "//home/cdc/test/versioned",
		Cluster:       os.Getenv("YT_PROXY"),
		CustomAttributes: map[string]string{
			"test":               "%true",
			"expiration_timeout": "604800000",
			"expiration_time":    "\"2200-01-12T03:32:51.298047Z\"",
		},
	}
}
