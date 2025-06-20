//go:build !disable_postgres_provider

package compositekey

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/internal/metrics"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/dblog"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/tests/helpers"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var (
	Source        = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix(""))
	testTableName = "text_int_pk"

	rowsAfterInserts = uint64(14)

	incrementalLimit = uint64(10)
	numberOfInserts  = 16

	sleepBetweenInserts = 100 * time.Millisecond

	minOutputItems = 15
	maxOutputItems = 30
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

func TestIncrementalSnapshot(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
		))
	}()
	sinkParams := Source.ToSinkParams()
	sink, err := postgres.NewSink(logger.Log, helpers.TransferID, sinkParams, helpers.EmptyRegistry())
	require.NoError(t, err)

	arrColSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "text_pk", DataType: ytschema.TypeString.String(), PrimaryKey: true},
		{ColumnName: "int_pk", DataType: ytschema.TypeInt32.String(), PrimaryKey: true},
	})
	changeItemBuilder := helpers.NewChangeItemsBuilder("public", testTableName, arrColSchema)

	require.NoError(t, sink.Push(changeItemBuilder.Inserts(t, []map[string]interface{}{{"text_pk": "11", "int_pk": 11}, {"text_pk": 12, "int_pk": 12}, {"text_pk": 13, "int_pk": 13}, {"text_pk": 14, "int_pk": 14}})))

	helpers.CheckRowsCount(t, Source, "public", testTableName, rowsAfterInserts)

	pgStorage, err := postgres.NewStorage(Source.ToStorageParams(nil))
	require.NoError(t, err)

	err = postgres.CreateReplicationSlot(&Source)
	require.NoError(t, err)

	src, err := postgres.NewSourceWrapper(
		&Source,
		Source.SlotID,
		nil,
		logger.Log,
		stats.NewSourceStats(metrics.NewRegistry()),
		coordinator.NewFakeClient(),
		true,
	)
	require.NoError(t, err)

	storage, err := dblog.NewStorage(logger.Log, src, pgStorage, pgStorage.Conn, incrementalLimit, Source.SlotID, "public", postgres.Represent)
	require.NoError(t, err)

	sourceTables, err := storage.TableList(nil)
	require.NoError(t, err)

	var compositeKeyTable *abstract.TableDescription = nil
	tables := sourceTables.ConvertToTableDescriptions()

	for _, table := range tables {
		if table.Name == testTableName {
			compositeKeyTable = &table
		}
	}

	require.NotNil(t, compositeKeyTable)

	var output []abstract.ChangeItem

	pusher := func(items []abstract.ChangeItem) error {
		output = append(output, items...)
		return nil
	}

	go func() {
		for i := 0; i < numberOfInserts; i++ {
			require.NoError(t, sink.Push(changeItemBuilder.Inserts(t, []map[string]interface{}{{"text_pk": fmt.Sprintf("%d", i), "int_pk": i}})))
			time.Sleep(sleepBetweenInserts)
		}
	}()

	err = storage.LoadTable(context.Background(), *compositeKeyTable, pusher)
	require.NoError(t, err)

	require.LessOrEqual(t, minOutputItems, len(output))
	require.LessOrEqual(t, len(output), maxOutputItems)
}
