package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

const ExpectedRowCount = 1000000

func CheckEntriesPerTable(t *testing.T, tableEntriesCounter map[string]int) {
	for _, val := range tableEntriesCounter {
		require.Equal(t, ExpectedRowCount, val)
	}
	require.Equal(t, 6, len(tableEntriesCounter))
}

func TestConnLimit1Worker4ThreadsSnapshotAndReplication(t *testing.T) {
	source := *pgrecipe.RecipeSource(
		pgrecipe.WithPrefix(""),
		pgrecipe.WithInitDir("init_source"),
		pgrecipe.WithEdit(func(pg *postgres.PgSource) {
			pg.User = "conn_test"
			pg.Password = "aA_12345"
		}),
	)
	source.WithDefaults()

	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: source.Port},
	))

	tableRowCounts := make(map[string]int)
	rwMutex := sync.RWMutex{}
	pushCallback := func(items []abstract.ChangeItem) error {
		for _, changeItem := range items {
			if changeItem.IsRowEvent() {
				rwMutex.Lock()
				tableRowCounts[changeItem.Table]++
				rwMutex.Unlock()
			}
		}
		return nil
	}

	sinker := &helpers.MockSink{PushCallback: pushCallback}
	target := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinker },
		Cleanup:       model.DisabledCleanup,
	}
	transfer1Worker4Threads := helpers.MakeTransfer("fake", &source, &target, abstract.TransferTypeSnapshotAndIncrement)
	transfer1Worker4Threads.Runtime = &abstract.LocalRuntime{ShardingUpload: abstract.ShardUploadParams{JobCount: 1, ProcessCount: 4}}
	worker := helpers.Activate(t, transfer1Worker4Threads)
	defer worker.Close(t)

	CheckEntriesPerTable(t, tableRowCounts)
	ctx := context.Background()
	writerString := fmt.Sprintf(
		"host=localhost port=%d dbname=%s user=writer password=aA_12345",
		helpers.GetIntFromEnv("PG_LOCAL_PORT"),
		os.Getenv("PG_LOCAL_DATABASE"),
	)
	srcConn, err := pgx.Connect(ctx, writerString)
	require.NoError(t, err)
	defer srcConn.Close(ctx)

	counter := 0
	start := time.Now()
	ticker := time.NewTicker(time.Millisecond * 100)
	for tickTime := range ticker.C {
		if tickTime.Sub(start) > time.Second*30 {
			ticker.Stop()
			break
		}
		_, err = srcConn.Exec(ctx, `INSERT INTO public.test1 (value) VALUES (12345678);`) //nolint
		require.NoError(t, err)
		counter++
	}
	err = helpers.WaitCond(time.Second*30, func() bool {
		rwMutex.RLock()
		res := tableRowCounts["test1"] == ExpectedRowCount+counter
		rwMutex.RUnlock()
		return res
	})
	require.NoError(t, err)
	require.Equal(t, ExpectedRowCount+counter, tableRowCounts["test1"])
}
