package main

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var (
	SourceNoCollapse = *pgrecipe.RecipeSource(
		pgrecipe.WithPrefix(""),
		pgrecipe.WithInitDir("init_source"),
		pgrecipe.WithEdit(func(pg *postgres.PgSource) {
			pg.CollapseInheritTables = false
			pg.UseFakePrimaryKey = true // PK constraint for partitioned tables is disabled for PostgreSQL < 12
			pg.SlotID = "testslot_no_collapse"
		}),
	)

	SourceCollapse = *pgrecipe.RecipeSource(
		pgrecipe.WithPrefix(""),
		pgrecipe.WithInitDir("init_source"),
		pgrecipe.WithEdit(func(pg *postgres.PgSource) {
			pg.CollapseInheritTables = true
			pg.UseFakePrimaryKey = true // PK constraint for partitioned tables is disabled for PostgreSQL < 12
			pg.SlotID = "testslot_collapse"
		}),
	)
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	SourceNoCollapse.WithDefaults()
	SourceCollapse.WithDefaults()
}

func splitByTables(items []abstract.ChangeItem) map[abstract.TableID][]abstract.ChangeItem {
	splited := make(map[abstract.TableID][]abstract.ChangeItem)
	for _, item := range items {
		id := item.TableID()
		splited[id] = append(splited[id], item)
	}
	return splited
}

func splitByKind(items []abstract.ChangeItem) map[abstract.Kind][]abstract.ChangeItem {
	splited := make(map[abstract.Kind][]abstract.ChangeItem)
	for _, item := range items {
		kind := item.Kind
		splited[kind] = append(splited[kind], item)
	}
	return splited
}

func waitForLoaded(t *testing.T, v *[]abstract.ChangeItem, mux *sync.Mutex, expectedSize int, duration time.Duration) {
	st := time.Now()
	for time.Since(st) < duration {
		mux.Lock()
		fmt.Println(len(*v))
		if len(*v) == expectedSize {
			logger.Log.Infof("SUCCESSULLY WAITED, expected: %v, actual: %v, waiting for: %v", expectedSize, len(*v), time.Since(st))
			mux.Unlock()
			return
		}
		logger.Log.Infof("WAITING, expected: %v, actual: %v, waiting for: %v", expectedSize, len(*v), time.Since(st))
		mux.Unlock()

		time.Sleep(time.Second)
	}
	t.Fail()
	require.Fail(t, "waitForLoaded")
}

func TestSnapshotAndIncrement(t *testing.T) {
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: SourceNoCollapse.Port},
	))

	partitionedTable := *abstract.NewTableID("public", "log_table_declarative_partitioning")
	part01Table := *abstract.NewTableID("public", "log_table_partition_y2022m01")
	part02Table := *abstract.NewTableID("public", "log_table_partition_y2022m02")
	parentTable := *abstract.NewTableID("public", "log_table_inheritance_partitioning")
	child01Table := *abstract.NewTableID("public", "log_table_descendant_y2022m01")
	child02Table := *abstract.NewTableID("public", "log_table_descendant_y2022m02")

	// no_collapse (CollapseInheritTables = false)

	sinkerNoCollapse := mocksink.NewMockSink(nil)
	sinkerNoCollapseMutex := sync.Mutex{}
	targetNoCollapse := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinkerNoCollapse },
		Cleanup:       model.Drop,
	}
	transferNoCollapse := helpers.MakeTransfer("fake_no_collapse", &SourceNoCollapse, &targetNoCollapse, abstract.TransferTypeSnapshotAndIncrement)

	var changeItemsNoCollapse []abstract.ChangeItem
	sinkerNoCollapse.PushCallback = func(input []abstract.ChangeItem) error {
		sinkerNoCollapseMutex.Lock()
		defer sinkerNoCollapseMutex.Unlock()
		for _, i := range input {
			// DEBUG
			logger.Log.Infof("timmyb32rQQQ::PUSH::%s", i.ToJSONString())
			// DEBUG
			if i.Table == "__consumer_keeper" {
				continue
			}
			changeItemsNoCollapse = append(changeItemsNoCollapse, i)
		}
		return nil
	}

	worker1 := helpers.Activate(t, transferNoCollapse)
	defer worker1.Close(t)

	waitForLoaded(t, &changeItemsNoCollapse, &sinkerNoCollapseMutex, 36, 30*time.Second)
	fmt.Printf("Transfer without collapse: snapshot changeItem dump(%v): %v\n", len(changeItemsNoCollapse), changeItemsNoCollapse)

	tableItemsNoCollapse := splitByTables(changeItemsNoCollapse)
	require.Equal(t, 4, len(tableItemsNoCollapse))                   // [log_table_descendant_y2022m01,log_table_descendant_y2022m02,log_table_partition_y2022m01,log_table_partition_y2022m02] - other skipped
	require.Equal(t, 36, len(changeItemsNoCollapse))                 // for every from these 4 tables: [drop_table, init_sharded_table_load, init_load_table, 4xinsert, done_load_table, done_sharded_table_load]
	require.Equal(t, 0, len(tableItemsNoCollapse[partitionedTable])) // partitionedTable not present in dst
	require.Equal(t, 9, len(tableItemsNoCollapse[part01Table]))
	require.Equal(t, 9, len(tableItemsNoCollapse[part02Table]))
	require.Equal(t, 0, len(tableItemsNoCollapse[parentTable])) // partitionedTable not present in dst
	require.Equal(t, 9, len(tableItemsNoCollapse[child01Table]))
	require.Equal(t, 9, len(tableItemsNoCollapse[child02Table]))

	part01KindsNoCollapse := splitByKind(tableItemsNoCollapse[part01Table])
	require.Equal(t, 6, len(part01KindsNoCollapse))
	require.Equal(t, 1, len(part01KindsNoCollapse[abstract.DropTableKind]))
	require.Equal(t, 1, len(part01KindsNoCollapse[abstract.InitShardedTableLoad]))
	require.Equal(t, 1, len(part01KindsNoCollapse[abstract.InitTableLoad]))
	require.Equal(t, 1, len(part01KindsNoCollapse[abstract.DoneTableLoad]))
	require.Equal(t, 1, len(part01KindsNoCollapse[abstract.DoneShardedTableLoad]))
	require.Equal(t, 4, len(part01KindsNoCollapse[abstract.InsertKind]))

	// collapse (CollapseInheritTables = true)

	sinkerCollapse := mocksink.NewMockSink(nil)
	sinkerCollapseMutex := sync.Mutex{}
	targetCollapse := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinkerCollapse },
		Cleanup:       model.Drop,
	}
	transferCollapse := helpers.MakeTransfer("fake_collapse", &SourceCollapse, &targetCollapse, abstract.TransferTypeSnapshotAndIncrement)

	var changeItemsCollapse []abstract.ChangeItem
	sinkerCollapse.PushCallback = func(input []abstract.ChangeItem) error {
		sinkerCollapseMutex.Lock()
		defer sinkerCollapseMutex.Unlock()
		for _, i := range input {
			if i.Table == "__consumer_keeper" {
				continue
			}
			logger.Log.Infof("QQQ::EL::%s", i.ToJSONString())
			changeItemsCollapse = append(changeItemsCollapse, i)
		}
		return nil
	}

	worker2 := helpers.Activate(t, transferCollapse)
	defer worker2.Close(t)

	waitForLoaded(t, &changeItemsCollapse, &sinkerCollapseMutex, 30, 30*time.Second)
	fmt.Printf("Transfer with collapse: snapshot changeItem dump(%v): %v\n", len(changeItemsCollapse), changeItemsCollapse)
	for _, v := range changeItemsCollapse {
		logger.Log.Infof("    snapshot changeItem dump item: %s", v.ToJSONString())
	}

	tableItemsCollapse := splitByTables(changeItemsCollapse)
	require.Equal(t, 2, len(tableItemsCollapse))
	require.Equal(t, 30, len(changeItemsCollapse)) // 2 drop_table, 2 init_sharded_table_load, 4 init_load_table, 4 done_load_table, 2 done_sharded_table_load, 16 data events
	require.Equal(t, 15, len(tableItemsCollapse[partitionedTable]))
	require.Equal(t, 15, len(tableItemsCollapse[parentTable]))

	parentKindsCollapse := splitByKind(tableItemsCollapse[parentTable])
	require.Equal(t, 6, len(parentKindsCollapse))
	require.Equal(t, 1, len(parentKindsCollapse[abstract.DropTableKind]))
	require.Equal(t, 1, len(parentKindsCollapse[abstract.InitShardedTableLoad]))
	require.Equal(t, 2, len(parentKindsCollapse[abstract.InitTableLoad]))
	require.Equal(t, 2, len(parentKindsCollapse[abstract.DoneTableLoad]))
	require.Equal(t, 1, len(parentKindsCollapse[abstract.DoneShardedTableLoad]))
	require.Equal(t, 8, len(parentKindsCollapse[abstract.InsertKind]))

	//---

	sinkToSource, err := postgres.NewSink(logger.Log, helpers.TransferID, SourceCollapse.ToSinkParams(), helpers.EmptyRegistry())
	require.NoError(t, err)

	schema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "id", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
		{ColumnName: "logdate", DataType: ytschema.TypeDate.String(), PrimaryKey: false},
		{ColumnName: "msg", DataType: ytschema.TypeString.String(), PrimaryKey: false},
	})
	valuesForPartitions := []map[string]interface{}{
		{"id": 100, "logdate": "2022-01-07", "msg": "repl_msg"},
		{"id": 101, "logdate": "2022-02-07", "msg": "repl_msg"},
		{"id": 102, "logdate": "2022-02-08", "msg": "repl_msg"},
	}

	changeItemBuilderPartitioned := helpers.NewChangeItemsBuilder("public", "log_table_declarative_partitioning", schema)
	changeItemBuilderParent := helpers.NewChangeItemsBuilder("public", "log_table_inheritance_partitioning", schema)
	require.NoError(t, sinkToSource.Push(changeItemBuilderPartitioned.Inserts(t, valuesForPartitions)))
	require.NoError(t, sinkToSource.Push(changeItemBuilderParent.Inserts(t, valuesForPartitions)))

	waitForLoaded(t, &changeItemsNoCollapse, &sinkerNoCollapseMutex, 36+6, 30*time.Second)
	fmt.Println("Replication without collapse is synced")
	tableItemsNoCollapse = splitByTables(changeItemsNoCollapse[36:])
	require.Equal(t, 4, len(tableItemsNoCollapse))

	waitForLoaded(t, &changeItemsCollapse, &sinkerCollapseMutex, 30+6, 30*time.Second)
	fmt.Println("Replication with collapse is synced")

	tableItemsCollapse = splitByTables(changeItemsCollapse[30:])
	require.Equal(t, 2, len(tableItemsCollapse))
}
