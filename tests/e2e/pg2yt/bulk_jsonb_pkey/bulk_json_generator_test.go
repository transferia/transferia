package bulk

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/internal/metrics"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	yt_provider "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/terryid"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
	"go.ytsaurus.tech/library/go/core/log"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

func TestRunner(t *testing.T) {
	t.Run("CheckSimpleValidity", checkSimpleValidity)
	t.Run("CheckGeneratorSequenceUniqueness", checkGeneratorSequenceUniqueness)
	t.Run("CheckGeneratorIsDeterministic", checkGeneratorIsDeterministic)
	_, ytDest, cancel := initYt(t)

	targetPort, err := helpers.GetPortFromStr(ytDest.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	defer cancel()
	t.Run("PumpDatabaseToYt_NoCollisions", testFactoryPumpDatabaseToYt(ytDest, "bulk_jsonb_no_collision", WithoutCollisionGenerationRules))
	t.Run("PumpDatabaseToYt_WithCollisions", testFactoryPumpDatabaseToYt(ytDest, "bulk_jsonb_general", DefaultGenerationRules))
}

// Utilities

func newPgSource(tableName string) postgres.PgSource {
	src := postgres.PgSource{
		ClusterID: os.Getenv("PG_CLUSTER_ID"),
		Hosts:     []string{"localhost"},
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      helpers.GetIntFromEnv("PG_LOCAL_PORT"),
		DBTables:  []string{os.Getenv("PG_LOCAL_DATABASE") + "." + tableName},
		SlotID:    "test_slot_" + tableName,
	}
	src.WithDefaults()
	return src
}

type ValuesWithKind struct {
	vals []interface{}
	kind abstract.Kind
}

// Haskell-like map :: (a -> b) -> [a] -> [b]
func mapValuesToChangeItems(f func(ValuesWithKind) abstract.ChangeItem) func([]ValuesWithKind) []abstract.ChangeItem {
	return func(vwk []ValuesWithKind) []abstract.ChangeItem {
		var result []abstract.ChangeItem
		for _, l := range vwk {
			result = append(result, f(l))
		}
		return result
	}
}

func teardown(env *yttest.Env, p string) {
	err := env.YT.RemoveNode(
		env.Ctx,
		ypath.Path(p),
		&yt.RemoveNodeOptions{
			Recursive: true,
			Force:     true,
		},
	)
	if err != nil {
		logger.Log.Error("unable to delete test folder", log.Error(err))
	}
}

// initializes YT client and sinker config
// do not forget to call testTeardown when resources are not needed anymore
func initYt(t *testing.T) (testEnv *yttest.Env, testCfg yt_provider.YtDestinationModel, testTeardown func()) {
	env, cancel := yttest.NewEnv(t)
	cypressPath := "//home/cdc/test/TM-1893"
	cfg := yt_helpers.RecipeYtTarget(cypressPath)
	return env, cfg, func() {
		teardown(env, cypressPath) // do not drop table
		cancel()
	}
}

var whoMakesJSONbAsKeyQuestionMarkSchema = abstract.NewTableSchema([]abstract.ColSchema{
	{DataType: ytschema.TypeInt32.String(), ColumnName: "id", PrimaryKey: true},
	{DataType: ytschema.TypeAny.String(), OriginalType: "pg:jsonb", ColumnName: "jb", PrimaryKey: true},
	{DataType: ytschema.TypeInt32.String(), ColumnName: "value"},
})

// absolutely unreadable..... but this thing is a factory depending on pgSource that generates function that accepts only values and kinds and returns appropriate change items
func whoMakesJSONbAsKeyQuestionMarkItemGenerator(schema, table string) func([]ValuesWithKind) []abstract.ChangeItem {
	return mapValuesToChangeItems(func(vwk ValuesWithKind) abstract.ChangeItem {
		return abstract.ChangeItem{
			TableSchema:  whoMakesJSONbAsKeyQuestionMarkSchema,
			Kind:         vwk.kind,
			Schema:       schema,
			Table:        table,
			ColumnNames:  []string{"id", "jb", "value"},
			ColumnValues: vwk.vals,
		}
	})
}

func whoMakesJSONbAsKeyQuestionMarkOldKeysItemGenerator(schema, table string) func([]ValuesWithKind) []abstract.ChangeItem {
	return mapValuesToChangeItems(func(vwk ValuesWithKind) abstract.ChangeItem {
		return abstract.ChangeItem{
			TableSchema: whoMakesJSONbAsKeyQuestionMarkSchema,
			Kind:        vwk.kind,
			Schema:      schema,
			Table:       table,
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"id", "jb"},
				KeyTypes:  []string{"integer", "jsonb"},
				KeyValues: vwk.vals,
			},
		}
	})
}

func checkSimpleValidity(t *testing.T) {
	t.Parallel()
	jGen := NewJSONGenerator(nil)
	jsonSeq := jGen.generateSequence(100)
	for _, randJSON := range jsonSeq {
		isValid := json.Valid([]byte(randJSON))
		require.True(t, isValid, randJSON)
	}
}

func checkGeneratorSequenceUniqueness(t *testing.T) {
	t.Parallel()
	jGen := NewJSONGenerator(nil)
	seq := jGen.generateSequence(20000) // works even for 2000000 (until code hasn't been changed)
	sort.Slice(seq, func(i, j int) bool {
		return seq[i] < seq[j]
	})

	require.True(t, json.Valid([]byte(seq[0])))
	for i := 0; i < len(seq)-1; i++ {
		require.True(t, json.Valid([]byte(seq[i+1])), seq[i+1])
		if seq[i] == seq[i+1] {
			t.Errorf("equal values at index^ %d: %v", i, seq[i])
		}
	}
}

func checkGeneratorIsDeterministic(t *testing.T) {
	t.Parallel()
	jGen := NewJSONGenerator(nil)
	jsonSeq := jGen.generateSequence(127)
	jsonSeq2 := jGen.generateSequence(127)
	require.Equal(t, jsonSeq, jsonSeq2, "generator should produce the same sequence with different calls")
}

func fillDatabaseWithJSONChangeItems(t *testing.T, source *postgres.PgSource, table string, jGen *JSONGenerator, jsonAmount int, shouldDrop bool, kind abstract.Kind) {
	t.Helper()

	pgSinkParams := source.ToSinkParams()
	pgSinkParams.SetMaintainTables(true)
	pgSinker, err := postgres.NewSink(logger.Log, terryid.GenerateTransferID(), pgSinkParams, metrics.NewRegistry())
	if pgSinker == nil {
		t.Fatal("couldn't create Postgresql sinker")
	}
	defer pgSinker.Close()
	if err != nil {
		t.Fatal(err)
	}

	if shouldDrop {
		// drop corresponding table
		dropTableChangeItem := whoMakesJSONbAsKeyQuestionMarkItemGenerator(source.Database, table)([]ValuesWithKind{{[]interface{}{0, "", 0}, abstract.DropTableKind}})
		err = pgSinker.Push(dropTableChangeItem)
		if err != nil {
			t.Fatal(err)
		}
	}

	jsons := jGen.generateSequence(jsonAmount)
	var changeItems []abstract.ChangeItem

	inserter := whoMakesJSONbAsKeyQuestionMarkItemGenerator(source.Database, table)
	if kind == abstract.UpdateKind || kind == abstract.DeleteKind {
		// use special item generator when deleting
		inserter = whoMakesJSONbAsKeyQuestionMarkOldKeysItemGenerator(source.Database, table)
	}
	for _, jsonStr := range jsons {
		changeItems = append(changeItems, inserter([]ValuesWithKind{
			// pusher should build kind=Delete query disregarding random integer
			{[]interface{}{0, jsonStr, rand.Int31()}, kind},
		})...)
	}
	err = pgSinker.Push(changeItems)
	if err != nil {
		logger.Log.Error("error filling test PG base", log.Error(err))
	}
}

func testFactoryPumpDatabaseToYt(ytDest yt_provider.YtDestinationModel, table string, generationRules GenerationRules) func(*testing.T) {
	pgSource := newPgSource(table)

	return func(t *testing.T) {
		defer func() {
			require.NoError(t, helpers.CheckConnections(
				helpers.LabeledPort{Label: "PG source", Port: pgSource.Port},
			))
		}()

		transfer := helpers.MakeTransfer(terryid.GenerateTransferID(), &pgSource, ytDest, abstract.TransferTypeSnapshotAndIncrement)

		tablePath := ypath.Path(ytDest.Path()).Child(pgSource.Database + "_" + table)

		// maximum amount of jsonb items
		handicap := 200

		// create fresh DB table
		logger.Log.Info("Create fresh DB table in postgres", log.String("table", table), log.Any("tablePath", tablePath))
		jsonGenerator := NewJSONGenerator(generationRules)
		// at least one item is required for table creation. These half of the items will be updated with replication procedure
		fillDatabaseWithJSONChangeItems(t, &pgSource, table, jsonGenerator, handicap/2, true, abstract.InsertKind)

		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Second)
		defer cancel()

		// create replication slot
		logger.Log.Info("Create replication slot", log.String("table", table), log.Any("tablePath", tablePath))
		err := postgres.CreateReplicationSlot(&pgSource)
		require.NoError(t, err)

		logger.Log.Info("Load snapshot", log.String("table", table), log.Any("tablePath", tablePath))
		snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
		err = snapshotLoader.LoadSnapshot(ctx)
		require.NoError(t, err)

		// launch replicaion worker
		logger.Log.Info("Start replication worker", log.String("table", table), log.Any("tablePath", tablePath))
		wrk := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, solomon.NewRegistry(nil), logger.Log)

		workerErrCh := make(chan error, 1)
		go func() {
			logger.Log.Info("Worker started", log.String("table", table), log.Any("tablePath", tablePath))
			workerErrCh <- wrk.Run()
		}()

		// upload some change items
		logger.Log.Info("Insert new change items into the database", log.String("table", table), log.Any("tablePath", tablePath))
		fillDatabaseWithJSONChangeItems(t, &pgSource, table, jsonGenerator, handicap, false, abstract.InsertKind)

		// table size should be handicap size
		logger.Log.Info(fmt.Sprintf("Wait for expected %d rows", handicap), log.String("table", table), log.Any("tablePath", tablePath))
		require.NoError(t, helpers.WaitEqualRowsCount(t, "postgres", table, helpers.GetSampleableStorageByModel(t, pgSource), helpers.GetSampleableStorageByModel(t, ytDest.LegacyModel()), 60*time.Second))

		// then remove the same amount of items from table
		logger.Log.Info("Put kind=remove change items into the database", log.String("table", table), log.Any("tablePath", tablePath))
		fillDatabaseWithJSONChangeItems(t, &pgSource, table, jsonGenerator, handicap, false, abstract.DeleteKind)

		// table size should be zero
		logger.Log.Info("Wait for expected 0 rows", log.String("table", table), log.Any("tablePath", tablePath))
		require.NoError(t, helpers.WaitEqualRowsCount(t, "postgres", table, helpers.GetSampleableStorageByModel(t, pgSource), helpers.GetSampleableStorageByModel(t, ytDest.LegacyModel()), 60*time.Second))

		// wait worker for finish
		logger.Log.Info("Wait for worker to finish", log.String("table", table), log.Any("tablePath", tablePath))
		if err := wrk.Stop(); err != nil {
			logger.Log.Error("Worker stop error", log.Error(err), log.String("table", table), log.Any("tablePath", tablePath))
		}
		require.NoError(t, <-workerErrCh)

		logger.Log.Info("Test done!", log.String("table", table), log.Any("tablePath", tablePath))
	}
}
