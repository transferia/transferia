package main

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	"github.com/transferria/transferria/tests/helpers"
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
			logger.Log.Infof("expected: %v, actual: %v, waiting for: %v", expectedSize, len(*v), time.Since(st))
			mux.Unlock()
			return
		}
		logger.Log.Infof("expected: %v, actual: %v, waiting for: %v", expectedSize, len(*v), time.Since(st))
		mux.Unlock()

		time.Sleep(time.Second)
	}
	t.Fail()
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

	sinkerNoCollapse := &helpers.MockSink{}
	sinkerNoCollapseMutex := sync.Mutex{}
	targetNoCollapse := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinkerNoCollapse },
		Cleanup:       model.Drop,
	}
	transferNoCollapse := helpers.MakeTransfer("fake_no_collapse", &SourceNoCollapse, &targetNoCollapse, abstract.TransferTypeSnapshotAndIncrement)

	var changeItemsNoCollapse []abstract.ChangeItem
	sinkerNoCollapse.PushCallback = func(input []abstract.ChangeItem) {
		sinkerNoCollapseMutex.Lock()
		defer sinkerNoCollapseMutex.Unlock()
		for _, i := range input {
			if i.Table == "__consumer_keeper" {
				continue
			}
			changeItemsNoCollapse = append(changeItemsNoCollapse, i)
		}
	}

	helpers.Activate(t, transferNoCollapse)
	waitForLoaded(t, &changeItemsNoCollapse, &sinkerNoCollapseMutex, 46, 30*time.Second)
	fmt.Printf("Transfer without collapse: snapshot changeItem dump(%v): %v\n", len(changeItemsNoCollapse), changeItemsNoCollapse)

	tableItemsNoCollapse := splitByTables(changeItemsNoCollapse)
	require.Equal(t, 6, len(tableItemsNoCollapse))
	require.Equal(t, 46, len(changeItemsNoCollapse))
	require.Equal(t, 5, len(tableItemsNoCollapse[partitionedTable])) // init/done + 3 row
	require.Equal(t, 9, len(tableItemsNoCollapse[part01Table]))
	require.Equal(t, 9, len(tableItemsNoCollapse[part02Table]))
	require.Equal(t, 5, len(tableItemsNoCollapse[parentTable]))
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

	sinkerCollapse := &helpers.MockSink{}
	sinkerCollapseMutex := sync.Mutex{}
	targetCollapse := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinkerCollapse },
		Cleanup:       model.Drop,
	}
	transferCollapse := helpers.MakeTransfer("fake_collapse", &SourceCollapse, &targetCollapse, abstract.TransferTypeSnapshotAndIncrement)

	var changeItemsCollapse []abstract.ChangeItem
	sinkerCollapse.PushCallback = func(input []abstract.ChangeItem) {
		sinkerCollapseMutex.Lock()
		defer sinkerCollapseMutex.Unlock()
		for _, i := range input {
			if i.Table == "__consumer_keeper" {
				continue
			}
			changeItemsCollapse = append(changeItemsCollapse, i)
		}
	}

	helpers.Activate(t, transferCollapse)
	waitForLoaded(t, &changeItemsCollapse, &sinkerCollapseMutex, 46, 30*time.Second)
	fmt.Printf("Transfer with collapse: snapshot changeItem dump(%v): %v\n", len(changeItemsCollapse), changeItemsCollapse)

	tableItemsCollapse := splitByTables(changeItemsCollapse)
	require.Equal(t, 2, len(tableItemsCollapse))
	require.Equal(t, 46, len(changeItemsCollapse))
	require.Equal(t, 23, len(tableItemsCollapse[partitionedTable]))
	require.Equal(t, 23, len(tableItemsCollapse[parentTable]))

	parentKindsCollapse := splitByKind(tableItemsCollapse[parentTable])
	require.Equal(t, 6, len(parentKindsCollapse))
	require.Equal(t, 3, len(parentKindsCollapse[abstract.DropTableKind]))
	require.Equal(t, 3, len(parentKindsCollapse[abstract.InitShardedTableLoad]))
	require.Equal(t, 3, len(parentKindsCollapse[abstract.InitTableLoad]))
	require.Equal(t, 3, len(parentKindsCollapse[abstract.DoneTableLoad]))
	require.Equal(t, 3, len(parentKindsCollapse[abstract.DoneShardedTableLoad]))
	require.Equal(t, 8, len(parentKindsCollapse[abstract.InsertKind]))

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

	waitForLoaded(t, &changeItemsNoCollapse, &sinkerNoCollapseMutex, 46+6, 30*time.Second)
	fmt.Println("Replication without collapse is synced")
	tableItemsNoCollapse = splitByTables(changeItemsNoCollapse[46:])
	require.Equal(t, len(tableItemsNoCollapse), 4)

	waitForLoaded(t, &changeItemsCollapse, &sinkerCollapseMutex, 46+6, 30*time.Second)
	fmt.Println("Replication with collapse is synced")

	tableItemsCollapse = splitByTables(changeItemsCollapse[46:])
	require.Equal(t, len(tableItemsCollapse), 2)
}
