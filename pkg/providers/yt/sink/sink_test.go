package sink

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/internal/metrics"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	client2 "github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	yt2 "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/recipe"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

func TestSnapshotToReplica(t *testing.T) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()
	defer teardown(env.YT, "//home/cdc/test/TM-1291")
	schema_ := abstract.NewTableSchema([]abstract.ColSchema{{DataType: "int32", ColumnName: "id", PrimaryKey: true}, {DataType: "any", ColumnName: "val"}})
	cfg := yt2.NewYtDestinationV1(yt2.YtDestination{
		Path:          "//home/cdc/test/TM-1291",
		Cluster:       os.Getenv("YT_PROXY"),
		CellBundle:    "default",
		PrimaryMedium: "default",
	})
	cfg.WithDefaults()
	table, err := newSinker(cfg, "some_uniq_transfer_id", 0, logger.Log, metrics.NewRegistry(), client2.NewFakeClient())
	require.NoError(t, err)
	require.NoError(t, table.Push([]abstract.ChangeItem{
		{
			LSN:    5,
			Kind:   abstract.InitShardedTableLoad,
			Schema: "foo",
			Table:  "bar",
		},
	}))
	require.NoError(t, table.Push([]abstract.ChangeItem{
		{
			TableSchema: schema_,
			LSN:         5,
			Kind:        abstract.InitTableLoad,
			Schema:      "foo",
			Table:       "bar",
		},
	}))
	stat, err := table.loadTableStatus()
	require.NoError(t, err)
	require.Equal(t, TableProgress{
		TransferID: "some_uniq_transfer_id",
		Table:      "foo_bar",
		LSN:        5,
		Status:     Snapshot,
	}, stat["foo_bar"])
	require.NoError(t, table.Push([]abstract.ChangeItem{
		{
			TableSchema:  schema_,
			LSN:          5,
			Kind:         abstract.InsertKind,
			Schema:       "foo",
			Table:        "bar",
			ColumnNames:  []string{"id", "val"},
			ColumnValues: []interface{}{int32(1), "old"},
		},
		{
			TableSchema:  schema_,
			LSN:          5,
			Kind:         abstract.InsertKind,
			Schema:       "foo",
			Table:        "bar",
			ColumnNames:  []string{"id", "val"},
			ColumnValues: []interface{}{int32(2), "old"},
		},
	}))
	require.NoError(t, table.Push([]abstract.ChangeItem{
		{
			TableSchema: schema_,
			LSN:         5,
			Kind:        abstract.DoneTableLoad,
			Schema:      "foo",
			Table:       "bar",
		},
	}))
	require.NoError(t, table.Push([]abstract.ChangeItem{
		{
			LSN:    5,
			Kind:   abstract.DoneShardedTableLoad,
			Schema: "foo",
			Table:  "bar",
		},
	}))
	stat, err = table.loadTableStatus()
	require.NoError(t, err)
	require.Equal(t, TableProgress{
		TransferID: "some_uniq_transfer_id",
		Table:      "foo_bar",
		LSN:        5,
		Status:     SyncWait,
	}, stat["foo_bar"])
	require.NoError(t, table.Push([]abstract.ChangeItem{
		{
			TableSchema:  schema_,
			LSN:          1,
			Kind:         abstract.InsertKind,
			Schema:       "foo",
			Table:        "bar",
			ColumnNames:  []string{"id", "val"},
			ColumnValues: []interface{}{int32(1), "new"}, // should be skipped
		},
		{
			TableSchema:  schema_,
			LSN:          10,
			Kind:         abstract.InsertKind,
			Schema:       "foo",
			Table:        "bar",
			ColumnNames:  []string{"id", "val"},
			ColumnValues: []interface{}{int32(2), "new"},
		},
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rows, err := env.YT.SelectRows(ctx, fmt.Sprintf("* from [%v/foo_bar]", cfg.Path()), nil)
	require.NoError(t, err)
	type fooBar struct {
		ID  int32  `yson:"id"`
		Val string `yson:"val"`
	}
	var res []fooBar
	for rows.Next() {
		var row fooBar
		require.NoError(t, rows.Scan(&row))
		res = append(res, row)
	}
	require.Equal(t, []fooBar{{1, "old"}, {2, "new"}}, res)
	stat, err = table.loadTableStatus()
	require.NoError(t, err)
	require.Equal(t, TableProgress{
		TransferID: "some_uniq_transfer_id",
		Table:      "foo_bar",
		LSN:        10,
		Status:     Synced,
	}, stat["foo_bar"])
}

func TestRotate(t *testing.T) {
	dirPath := ypath.Path("//home/cdc/test/DTSUPPORT-786")
	env, cancel := recipe.NewEnv(t)
	defer cancel()
	defer teardown(env.YT, dirPath)
	cfg := yt2.NewYtDestinationV1(yt2.YtDestination{
		Path:          dirPath.String(),
		Cluster:       os.Getenv("YT_PROXY"),
		CellBundle:    "default",
		PrimaryMedium: "default",
		Rotation: &model.RotatorConfig{
			KeepPartCount:     10,
			PartType:          "d",
			PartSize:          1,
			TimeColumn:        "dt",
			TableNameTemplate: "",
		},
	})
	cfg.WithDefaults()
	table, err := newSinker(cfg, "some_uniq_transfer_id", 0, logger.Log, metrics.NewRegistry(), client2.NewFakeClient())
	require.NoError(t, err)

	rowBuilder := func(schema_ *abstract.TableSchema, table string) func(id int32, val interface{}, dt interface{}) abstract.ChangeItem {
		return func(id int32, val interface{}, dt interface{}) abstract.ChangeItem {
			return abstract.ChangeItem{
				TableSchema:  schema_,
				LSN:          1,
				Kind:         abstract.InsertKind,
				Schema:       "",
				Table:        table,
				ColumnNames:  []string{"id", "val", "dt"},
				ColumnValues: []interface{}{id, val, dt},
			}
		}
	}
	t.Run("string_dt", func(t *testing.T) {
		schema_ := abstract.NewTableSchema([]abstract.ColSchema{{DataType: string(yt_schema.TypeInt32), ColumnName: "id", PrimaryKey: true}, {DataType: string(yt_schema.TypeAny), ColumnName: "val"}, {DataType: string(yt_schema.TypeBytes), ColumnName: "dt"}})
		require.NoError(t, table.Push([]abstract.ChangeItem{
			rowBuilder(schema_, "string_dt")(1, map[string]interface{}{"a": 123}, "2012-01-01"),
			rowBuilder(schema_, "string_dt")(2, map[string]interface{}{"a": 124}, "2012-01-02"),
			rowBuilder(schema_, "string_dt")(2, map[string]interface{}{"a": 124}, "2012-01-03"),
		}))
		ytListNodeOptions := &yt.ListNodeOptions{Attributes: []string{"type", "path"}}
		var childNodes []YtRotationNode
		require.NoError(t, env.YT.ListNode(context.Background(), yt2.SafeChild(dirPath, "string_dt"), &childNodes, ytListNodeOptions))
		require.Len(t, childNodes, 3)
	})
	t.Run("time_dt", func(t *testing.T) {
		schema_ := abstract.NewTableSchema([]abstract.ColSchema{{DataType: string(yt_schema.TypeInt32), ColumnName: "id", PrimaryKey: true}, {DataType: string(yt_schema.TypeAny), ColumnName: "val"}, {DataType: string(yt_schema.TypeDatetime), ColumnName: "dt"}})
		require.NoError(t, table.Push([]abstract.ChangeItem{
			rowBuilder(schema_, "time_dt")(1, map[string]interface{}{"a": 123}, time.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC)),
			rowBuilder(schema_, "time_dt")(2, map[string]interface{}{"a": 124}, time.Date(2012, 1, 2, 0, 0, 0, 0, time.UTC)),
			rowBuilder(schema_, "time_dt")(2, map[string]interface{}{"a": 124}, time.Date(2012, 1, 3, 0, 0, 0, 0, time.UTC)),
		}))
		ytListNodeOptions := &yt.ListNodeOptions{Attributes: []string{"type", "path"}}
		var childNodes []YtRotationNode
		require.NoError(t, env.YT.ListNode(context.Background(), yt2.SafeChild(dirPath, "time_dt"), &childNodes, ytListNodeOptions))
		require.Len(t, childNodes, 3)
	})
}

func TestPivotKeys(t *testing.T) {
	cfg := yt2.NewYtDestinationV1(yt2.YtDestination{
		Path:           "//home/cdc/test/TM-2919",
		Cluster:        os.Getenv("YT_PROXY"),
		CellBundle:     "default",
		PrimaryMedium:  "default",
		TimeShardCount: 10,
		HashColumn:     "pinhata",
	})
	cfg.WithDefaults()
	cols := []abstract.ColSchema{
		{
			DataType:   string(yt_schema.TypeString),
			ColumnName: "id",
			PrimaryKey: true,
		},
		{
			DataType:   string(yt_schema.TypeString),
			ColumnName: "val",
		},
	}
	s := NewSchema(cols, cfg, yt2.SafeChild(ypath.Path(cfg.Path()), "pinhatable"))
	pivotKeys := s.PivotKeys()
	require.Len(t, pivotKeys, 1)
	require.Empty(t, pivotKeys[0])

	cols[0].ColumnName = "pinhata"
	s = NewSchema(cols, cfg, yt2.SafeChild(ypath.Path(cfg.Path()), "pinhatable"))
	pivotKeys = s.PivotKeys()
	require.Len(t, pivotKeys, 11)
}

func shardingTestHelper(t *testing.T, hashCol string, uid string, dirPath string, expected interface{}) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()
	defer teardown(env.YT, ypath.Path(dirPath))
	cfg := yt2.NewYtDestinationV1(yt2.YtDestination{
		Path:                     dirPath,
		Cluster:                  os.Getenv("YT_PROXY"),
		CellBundle:               "default",
		PrimaryMedium:            "default",
		TimeShardCount:           10,
		HashColumn:               hashCol,
		UseStaticTableOnSnapshot: false, // TM-4249
	})
	cfg.WithDefaults()
	table, err := newSinker(cfg, uid, 0, logger.Log, metrics.NewRegistry(), client2.NewFakeClient())
	require.NoError(t, err)
	tableSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{
			DataType:   string(yt_schema.TypeString),
			ColumnName: "id",
			PrimaryKey: true,
		},
		{
			DataType:   string(yt_schema.TypeString),
			ColumnName: "val",
		},
	})
	require.NoError(t, table.Push([]abstract.ChangeItem{
		{
			TableSchema: tableSchema,
			LSN:         5,
			Kind:        abstract.InitTableLoad,
			Schema:      "pinhaschema",
			Table:       "pinhatable",
		},
	}))
	require.NoError(t, table.Push([]abstract.ChangeItem{
		{
			TableSchema:  tableSchema,
			LSN:          5,
			Kind:         abstract.InsertKind,
			Schema:       "pinhaschema",
			Table:        "pinhatable",
			ColumnNames:  []string{"id", "val"},
			ColumnValues: []interface{}{"ruason_id", "di_nosaur"},
		},
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rows, err := env.YT.SelectRows(ctx, fmt.Sprintf("* from [%v/pinhaschema_pinhatable]", dirPath), nil)
	require.NoError(t, err)
	var res []interface{}
	for rows.Next() {
		var row interface{}
		require.NoError(t, rows.Scan(&row))
		res = append(res, row)
	}
	require.Equal(t, expected, res)
}

func TestSharding(t *testing.T) {
	hashCol := "id"
	uid := "unique_pinha_id"
	dirPath := "//home/cdc/test/TM-2919-1"
	expected := []interface{}{map[string]interface{}{"_shard_key": uint64(9), "id": "ruason_id", "val": "di_nosaur"}}
	shardingTestHelper(t, hashCol, uid, dirPath, expected)
}

func TestNoSharding(t *testing.T) {
	hashCol := "pinhata" // Won't be found among the available cols -> no sharding!
	uid := "pinha_unique_id"
	dirPath := "//home/cdc/test/TM-2919-2"
	expected := []interface{}{map[string]interface{}{"id": "ruason_id", "val": "di_nosaur"}}
	shardingTestHelper(t, hashCol, uid, dirPath, expected)
}

func TestLargeRowsWorkWithSpecialSinkOption(t *testing.T) {
	configs := []yt2.YtDestinationModel{
		yt2.NewYtDestinationV1(yt2.YtDestination{
			Path: "//home/cdc/test/ok/TM-5580-sorted",
		}),

		// This does not work due to https://st.yandex-team.ru/TM-5595
		//yt2.NewYtDestinationV1(yt2.YtDestination{
		//	Path:          "//home/cdc/test/ok/TM-5580-versioned",
		//	VersionColumn: "id",
		//}),
	}

	ytEnv, cancel := recipe.NewEnv(t)
	defer cancel()

	for _, cfg := range configs {
		ytModel := cfg.(*yt2.YtDestinationWrapper).Model
		ytModel.UseStaticTableOnSnapshot = false
		ytModel.LoseDataOnError = true
		ytModel.Cluster = os.Getenv("YT_PROXY")
		ytModel.CellBundle = "default"
		ytModel.PrimaryMedium = "default"
		cfg.WithDefaults()

		sink, err := NewSinker(cfg, "dtttm5880", 0, logger.Log, metrics.NewRegistry(), client2.NewFakeClient(), nil)
		require.NoError(t, err)
		require.NoError(t, sink.Push([]abstract.ChangeItem{makeLargeChangeItem()}))
		require.NoError(t, sink.Close())

		reader, err := ytEnv.YT.SelectRows(context.Background(), fmt.Sprintf("sum(1) from [%s/test] group by 1", ytModel.Path), &yt.SelectRowsOptions{})
		require.NoError(t, err)
		require.NoError(t, reader.Err())
		require.False(t, reader.Next()) // no rows => no groups of rows
		require.NoError(t, reader.Close())
	}
}

func TestLargeRowsDontWorkWithoutSpecialSinkOption(t *testing.T) {
	configs := []yt2.YtDestinationModel{
		yt2.NewYtDestinationV1(yt2.YtDestination{
			Path: "//home/cdc/test/fail/TM-5580-sorted",
		}),

		// This does not work due to https://st.yandex-team.ru/TM-5595
		//yt2.NewYtDestinationV1(yt2.YtDestination{
		//	Path:          "//home/cdc/test/fail/TM-5580-versioned",
		//	VersionColumn: "id",
		//}),
	}

	for _, cfg := range configs {
		ytModel := cfg.(*yt2.YtDestinationWrapper).Model
		ytModel.UseStaticTableOnSnapshot = false
		ytModel.LoseDataOnError = false
		ytModel.Cluster = os.Getenv("YT_PROXY")
		ytModel.CellBundle = "default"
		ytModel.PrimaryMedium = "default"
		cfg.WithDefaults()

		sink, err := NewSinker(cfg, "dtttm5880", 0, logger.Log, metrics.NewRegistry(), client2.NewFakeClient(), nil)
		require.NoError(t, err)
		require.Error(t, sink.Push([]abstract.ChangeItem{makeLargeChangeItem()}))
		require.NoError(t, sink.Close())
	}
}

func makeLargeChangeItem() abstract.ChangeItem {
	tableSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "id", DataType: string(yt_schema.TypeString), PrimaryKey: true},
		{ColumnName: "value", DataType: string(yt_schema.TypeString)},
		{ColumnName: "version", DataType: string(yt_schema.TypeInt64)},
	})
	colNames := yslices.Map(tableSchema.Columns(), func(colSchema abstract.ColSchema) string {
		return colSchema.ColumnName
	})
	const mib = 1024 * 1024
	return abstract.ChangeItem{
		ID:          1,
		LSN:         123,
		Kind:        abstract.InsertKind,
		Table:       "test",
		ColumnNames: colNames,
		TableSchema: tableSchema,
		ColumnValues: []interface{}{
			"1",
			strings.Repeat("x", 16*mib+1),
			1,
		},
	}
}
