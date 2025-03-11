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

func waitForLoaded(v *[]abstract.ChangeItem, mux *sync.Mutex, expectedSize int) {
	for {
		mux.Lock()
		fmt.Println(len(*v))
		if len(*v) >= expectedSize {
			mux.Unlock()
			break
		}
		mux.Unlock()

		time.Sleep(time.Second)
	}
}

func TestSnapshotAndIncrement(t *testing.T) {
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: SourceCollapse.Port},
	))

	sinkerNoCollapse := &helpers.MockSink{}
	sinkerNoCollapseMutex := sync.Mutex{}
	targetNoCollapse := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinkerNoCollapse },
		Cleanup:       model.Drop,
	}

	var result []abstract.ChangeItem
	sinkerNoCollapse.PushCallback = func(input []abstract.ChangeItem) {
		sinkerNoCollapseMutex.Lock()
		defer sinkerNoCollapseMutex.Unlock()
		for _, i := range input {
			if i.Table == "__consumer_keeper" {
				continue
			}
			if !i.IsRowEvent() {
				continue
			}
			result = append(result, i)
		}
	}

	transfer := helpers.MakeTransfer("data-objects", &SourceCollapse, &targetNoCollapse, abstract.TransferTypeSnapshotOnly)
	transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{"public.log_table_inheritance_partitioning", "public.log_table_declarative_partitioning"}}
	_ = helpers.Activate(t, transfer)
	for k, data := range splitByTables(result) {
		logger.Log.Infof("%s:\n%v", k.String(), abstract.Sniff(data))
		require.Equal(t, 8, len(data))
	}
	require.Len(t, result, 16)

	replicationTransfer := helpers.MakeTransfer("data-objects", &SourceCollapse, &targetNoCollapse, abstract.TransferTypeIncrementOnly)
	replicationTransfer.DataObjects = &model.DataObjects{IncludeObjects: []string{"public.log_table_inheritance_partitioning", "public.log_table_declarative_partitioning"}}
	w := helpers.Activate(t, replicationTransfer)
	sinkToSource, err := postgres.NewSink(logger.Log, helpers.TransferID, SourceCollapse.ToSinkParams(), helpers.EmptyRegistry())
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
	changeToBeSkippedParent := helpers.NewChangeItemsBuilder("public", "log_table_to_be_ignored", schema)
	require.NoError(t, sinkToSource.Push(changeItemBuilderPartitioned.Inserts(t, valuesForPartitions)))
	require.NoError(t, sinkToSource.Push(changeItemBuilderParent.Inserts(t, valuesForPartitions)))
	require.NoError(t, sinkToSource.Push(changeToBeSkippedParent.Inserts(t, valuesForPartitions)))
	require.NoError(t, err)

	waitForLoaded(&result, &sinkerNoCollapseMutex, 8+8+3+3)
	require.Len(t, splitByTables(result), 2) // should only include 2 tables
	w.Close(t)
	for k, data := range splitByTables(result) {
		logger.Log.Infof("%s:\n%v", k.String(), abstract.Sniff(data))
		require.Equal(t, 8+3, len(data))
	}
}
