package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers/changeitem"
	"github.com/transferia/transferia/tests/helpers/delivery"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	"github.com/transferia/transferia/tests/helpers/yatestx"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var (
	SourceNoCollapse = *pgrecipe.RecipeSource(
		pgrecipe.WithPrefix(""),
		pgrecipe.WithInitDir(yatestx.ProjectSource("init_source")),
		pgrecipe.WithEdit(func(pg *provider_postgres.PgSource) {
			pg.CollapseInheritTables = false
			pg.UseFakePrimaryKey = true // PK constraint for partitioned tables is disabled for PostgreSQL < 12
			pg.SlotID = "testslot_no_collapse"
		}),
	)

	SourceCollapse = *pgrecipe.RecipeSource(
		pgrecipe.WithPrefix(""),
		pgrecipe.WithInitDir(yatestx.ProjectSource("init_source")),
		pgrecipe.WithEdit(func(pg *provider_postgres.PgSource) {
			pg.CollapseInheritTables = true
			pg.UseFakePrimaryKey = true // PK constraint for partitioned tables is disabled for PostgreSQL < 12
			pg.SlotID = "testslot_collapse"
		}),
	)

	SourceCollapseDBLogEnabled = *pgrecipe.RecipeSource(
		pgrecipe.WithPrefix(""),
		pgrecipe.WithInitDir(yatestx.ProjectSource("init_source")),
		pgrecipe.WithEdit(func(pg *provider_postgres.PgSource) {
			pg.CollapseInheritTables = true
			pg.UseFakePrimaryKey = true // PK constraint for partitioned tables is disabled for PostgreSQL < 12
			pg.SlotID = "testslot_collapse_dblog_enabled"
			pg.DBLogEnabled = true
			pg.ChunkSize = 1
			pg.DBTables = []string{"public.log_table_declarative_partitioning"}
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

func requireAllNamesSame(t *testing.T, expectedName string, items []abstract.ChangeItem) {
	for _, item := range items {
		require.Equal(t, expectedName, fmt.Sprintf("%s.%s", item.TableID().Namespace, item.TableID().Name))
	}
}

func TestSnapshotAndIncrement(t *testing.T) {
	defer require.NoError(t, network.CheckConnections(
		network.LabeledPort{Label: "PG source", Port: SourceNoCollapse.Port},
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
	transferNoCollapse := transferhelpers.MakeTransfer("fake_no_collapse", &SourceNoCollapse, &targetNoCollapse, abstract.TransferTypeSnapshotAndIncrement)

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

	worker1 := delivery.Activate(t, transferNoCollapse)
	defer worker1.Close(t)

	waitForLoaded(t, &changeItemsNoCollapse, &sinkerNoCollapseMutex, 46, 30*time.Second)
	fmt.Printf("Transfer without collapse: snapshot changeItem dump(%v): %v\n", len(changeItemsNoCollapse), changeItemsNoCollapse)

	tableItemsNoCollapse := splitByTables(changeItemsNoCollapse)
	require.Equal(t, 6, len(tableItemsNoCollapse))                   // 4 children + 2 parent tables (parents read with ONLY, 0 data rows)
	require.Equal(t, 46, len(changeItemsNoCollapse))                 // 4 children × 9 + 2 parents × 5 (lifecycle only, no data rows via ONLY)
	require.Equal(t, 5, len(tableItemsNoCollapse[partitionedTable])) // parent table: drop + init_sharded + init_load + done_load + done_sharded (0 data rows via ONLY)
	require.Equal(t, 9, len(tableItemsNoCollapse[part01Table]))
	require.Equal(t, 9, len(tableItemsNoCollapse[part02Table]))
	require.Equal(t, 5, len(tableItemsNoCollapse[parentTable])) // parent table: drop + init_sharded + init_load + done_load + done_sharded (0 data rows via ONLY)
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
	transferCollapse := transferhelpers.MakeTransfer("fake_collapse", &SourceCollapse, &targetCollapse, abstract.TransferTypeSnapshotAndIncrement)

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

	worker2 := delivery.Activate(t, transferCollapse)
	defer worker2.Close(t)

	waitForLoaded(t, &changeItemsCollapse, &sinkerCollapseMutex, 34, 30*time.Second)
	fmt.Printf("Transfer with collapse: snapshot changeItem dump(%v): %v\n", len(changeItemsCollapse), changeItemsCollapse)
	for _, v := range changeItemsCollapse {
		logger.Log.Infof("    snapshot changeItem dump item: %s", v.ToJSONString())
	}

	tableItemsCollapse := splitByTables(changeItemsCollapse)
	require.Equal(t, 2, len(tableItemsCollapse))
	require.Equal(t, 34, len(changeItemsCollapse)) // 2 drop_table, 2 init_sharded_table_load, 6 init_load_table (3 shards each), 6 done_load_table, 2 done_sharded_table_load, 16 data events
	require.Equal(t, 17, len(tableItemsCollapse[partitionedTable]))
	require.Equal(t, 17, len(tableItemsCollapse[parentTable]))

	parentKindsCollapse := splitByKind(tableItemsCollapse[parentTable])
	require.Equal(t, 6, len(parentKindsCollapse))
	require.Equal(t, 1, len(parentKindsCollapse[abstract.DropTableKind]))
	require.Equal(t, 1, len(parentKindsCollapse[abstract.InitShardedTableLoad]))
	require.Equal(t, 3, len(parentKindsCollapse[abstract.InitTableLoad]))
	require.Equal(t, 3, len(parentKindsCollapse[abstract.DoneTableLoad]))
	require.Equal(t, 1, len(parentKindsCollapse[abstract.DoneShardedTableLoad]))
	require.Equal(t, 8, len(parentKindsCollapse[abstract.InsertKind]))

	//---

	sinkToSource, err := provider_postgres.NewSink(logger.Log, transferhelpers.TransferID, SourceCollapse.ToSinkParams(), testmetrics.EmptyRegistry())
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

	changeItemBuilderPartitioned := changeitem.NewChangeItemsBuilder("public", "log_table_declarative_partitioning", schema)
	changeItemBuilderParent := changeitem.NewChangeItemsBuilder("public", "log_table_inheritance_partitioning", schema)
	require.NoError(t, sinkToSource.Push(changeItemBuilderPartitioned.Inserts(t, valuesForPartitions)))
	require.NoError(t, sinkToSource.Push(changeItemBuilderParent.Inserts(t, valuesForPartitions)))

	waitForLoaded(t, &changeItemsNoCollapse, &sinkerNoCollapseMutex, 46+6, 30*time.Second)
	fmt.Println("Replication without collapse is synced")
	tableItemsNoCollapse = splitByTables(changeItemsNoCollapse[46:])
	require.Equal(t, 4, len(tableItemsNoCollapse))

	waitForLoaded(t, &changeItemsCollapse, &sinkerCollapseMutex, 34+6, 30*time.Second)
	fmt.Println("Replication with collapse is synced")

	tableItemsCollapse = splitByTables(changeItemsCollapse[34:])
	require.Equal(t, 2, len(tableItemsCollapse))

	// check new partition replication
	changeItemsCollapse = changeItemsCollapse[:0] // clear changeItemsCollapse

	srcConn, err := provider_postgres.MakeConnPoolFromSrc(&SourceCollapse, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	_, err = srcConn.Exec(context.Background(), "CREATE TABLE log_table_partition_y2022m03 PARTITION OF log_table_declarative_partitioning FOR VALUES FROM ('2022-03-01') TO ('2022-04-01');")
	require.NoError(t, err)

	_, err = srcConn.Exec(context.Background(), "INSERT INTO log_table_partition_y2022m03(id, logdate, msg) VALUES (103, '2022-03-07', 'repl_msg')")
	require.NoError(t, err)

	waitForLoaded(t, &changeItemsCollapse, &sinkerCollapseMutex, 1, 30*time.Second)

	// DBLog enabled (CollapseInheritTables = true)
	t.Run("dblog enabled", testDBLogEnabled)
}

func testDBLogEnabled(t *testing.T) {
	sinkerCollapse := &mocksink.MockSink{}
	sinkerCollapseMutex := sync.Mutex{}
	targetCollapse := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinkerCollapse },
		Cleanup:       model.Drop,
	}
	srcConn, err := provider_postgres.MakeConnPoolFromSrc(&SourceCollapse, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	schema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "id", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
		{ColumnName: "logdate", DataType: ytschema.TypeDate.String(), PrimaryKey: false},
		{ColumnName: "msg", DataType: ytschema.TypeString.String(), PrimaryKey: false},
	})
	valuesForPartitions := []map[string]interface{}{
		{"id": 300, "logdate": "2022-03-07", "msg": "repl_msg"},
		{"id": 400, "logdate": "2022-04-07", "msg": "repl_msg"},
		{"id": 401, "logdate": "2022-04-07", "msg": "repl_msg"},
		{"id": 402, "logdate": "2022-04-08", "msg": "repl_msg"},
		{"id": 403, "logdate": "2022-04-09", "msg": "repl_msg"},
	}
	changeItemBuilderParent := changeitem.NewChangeItemsBuilder("public", "log_table_declarative_partitioning", schema)
	sinkToSource, err := provider_postgres.NewSink(logger.Log, transferhelpers.TransferID, SourceCollapseDBLogEnabled.ToSinkParams(), testmetrics.EmptyRegistry())
	require.NoError(t, err)

	var changeItemsCollapse []abstract.ChangeItem
	once := sync.Once{}
	sinkerCollapse.PushCallback = func(input []abstract.ChangeItem) error {
		sinkerCollapseMutex.Lock()
		defer sinkerCollapseMutex.Unlock()
		for _, i := range input {
			if i.Table == "__consumer_keeper" {
				continue
			}
			logger.Log.Infof("DBLOG::EL::%s", i.ToJSONString())
			changeItemsCollapse = append(changeItemsCollapse, i)

			if len(changeItemsCollapse) >= 2 {
				once.Do(func() {
					_, err := srcConn.Exec(context.Background(), "CREATE TABLE log_table_partition_y2022m04 PARTITION OF log_table_declarative_partitioning FOR VALUES FROM ('2022-04-01') TO ('2022-05-01');")
					require.NoError(t, err)
					require.NoError(t, sinkToSource.Push(changeItemBuilderParent.Inserts(t, valuesForPartitions)))
					logger.Log.Infof("DBLOG::CREATED PARTITION log_table_partition_y2022m04")
				})
			}
		}
		return nil
	}

	transferDBLogEnabled := transferhelpers.MakeTransfer("fake_collapse_dblog_enabled", &SourceCollapseDBLogEnabled, &targetCollapse, abstract.TransferTypeSnapshotAndIncrement)
	workerDBLogEnabled := delivery.Activate(t, transferDBLogEnabled)
	defer workerDBLogEnabled.Close(t)

	waitForLoaded(t, &changeItemsCollapse, &sinkerCollapseMutex, 28, 30*time.Second) // 1 drop table + 12 values before snapshot + 5 * 3 values during snapshot
	// multiple inserts it`s from one replication stage and one from snapshot dblog stage, and 1 from replication stage
	requireAllNamesSame(t, SourceCollapseDBLogEnabled.DBTables[0], changeItemsCollapse)

	time.Sleep(5 * time.Second) // wait for snapshot stage to finish

	// insert for replication stage
	valuesForPartitionsNewPartition := []map[string]interface{}{
		{"id": 500, "logdate": "2022-05-07", "msg": "repl_msg"},
		{"id": 501, "logdate": "2022-05-07", "msg": "repl_msg"},
		{"id": 502, "logdate": "2022-05-08", "msg": "repl_msg"},
		{"id": 503, "logdate": "2022-05-09", "msg": "repl_msg"},
	}

	changeItemsCollapse = changeItemsCollapse[:0] // clear changeItemsCollapse
	_, err = srcConn.Exec(context.Background(), "CREATE TABLE log_table_partition_y2022m05 PARTITION OF log_table_declarative_partitioning FOR VALUES FROM ('2022-05-01') TO ('2022-06-01');")
	require.NoError(t, err)
	require.NoError(t, sinkToSource.Push(changeItemBuilderParent.Inserts(t, valuesForPartitionsNewPartition)))

	waitForLoaded(t, &changeItemsCollapse, &sinkerCollapseMutex, 4, 30*time.Second)
	requireAllNamesSame(t, SourceCollapseDBLogEnabled.DBTables[0], changeItemsCollapse)
}
