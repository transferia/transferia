package main

import (
	"context"
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
	"github.com/transferia/transferia/tests/helpers"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
)

var (
	Source = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("init_source"))
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

//---------------------------------------------------------------------------------------------------------------------
// this test ensures that a logical decoding message emitted via pg_logical_emit_message
// (both transactional and non-transactional) does not break the replication process:
// it should be silently ignored, and ordinary changes before/after it should still be
// delivered correctly.

func TestReplicationIgnoresLogicalMessage(t *testing.T) {
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: Source.Port},
	))

	//------------------------------------------------------------------------------
	// start replication

	sinker := mocksink.NewMockSink(nil)
	target := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinker },
		Cleanup:       model.DisabledCleanup,
	}
	transfer := helpers.MakeTransfer("fake", &Source, &target, abstract.TransferTypeSnapshotAndIncrement)

	mutex := sync.Mutex{}
	var changeItems []abstract.ChangeItem
	sinker.PushCallback = func(input []abstract.ChangeItem) error {
		mutex.Lock()
		defer mutex.Unlock()

		for _, el := range input {
			if el.Table != "__test" || el.Kind != abstract.InsertKind {
				continue
			}
			changeItems = append(changeItems, el)
		}

		return nil
	}

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	//-----------------------------------------------------------------------------------------------------------------
	// execute SQL statements: insert a row, emit logical messages, insert another row

	srcConn, err := provider_postgres.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	_, err = srcConn.Exec(context.Background(), `INSERT INTO __test (id, value) VALUES (1, 'before')`)
	require.NoError(t, err)

	// non-transactional logical message
	_, err = srcConn.Exec(context.Background(), `SELECT pg_logical_emit_message(false, 'test-prefix', 'non-transactional content')`)
	require.NoError(t, err)

	// transactional logical message
	tx, err := srcConn.Begin(context.Background())
	require.NoError(t, err)
	_, err = tx.Exec(context.Background(), `SELECT pg_logical_emit_message(true, 'test-prefix', 'transactional content')`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(context.Background()))

	_, err = srcConn.Exec(context.Background(), `INSERT INTO __test (id, value) VALUES (2, 'after')`)
	require.NoError(t, err)

	//-----------------------------------------------------------------------------------------------------------------
	// wait until both inserts arrive, proving that replication carried on smoothly across the logical messages

	for {
		mutex.Lock()
		if len(changeItems) >= 2 {
			mutex.Unlock()
			break
		}
		mutex.Unlock()
		time.Sleep(time.Second)
	}

	mutex.Lock()
	defer mutex.Unlock()

	require.Len(t, changeItems, 2)

	firstRow := changeItems[0].AsMap()
	require.EqualValues(t, 1, firstRow["id"])
	require.EqualValues(t, "before", firstRow["value"])

	secondRow := changeItems[1].AsMap()
	require.EqualValues(t, 2, secondRow["id"])
	require.EqualValues(t, "after", secondRow["value"])
}
