package tests

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
)

func insertToastValue(t *testing.T, conn *pgxpool.Pool, id int) {
	_, err := conn.Exec(t.Context(), fmt.Sprintf(`
		INSERT INTO test_toast_table (id, val, n)
		VALUES (%d, to_jsonb(repeat('X', 500000)::text), %d);
	`, id, id))
	require.NoError(t, err)
}

func waitForUpdate(t *testing.T, counter *int, expectedCount int, timeout time.Duration) {
	for {
		select {
		case <-time.After(timeout):
			require.Fail(t, "Timeout waiting for update")
		case <-time.Tick(1 * time.Second):
			if *counter == expectedCount {
				return
			}
		}
	}
}

func TestToastValuesFromOldKeys(t *testing.T) {
	source := *pgrecipe.RecipeSource(
		pgrecipe.WithPrefix(""),
		pgrecipe.WithInitDir("init_source"),
		pgrecipe.WithEdit(func(pg *postgres.PgSource) {
			pg.SlotID = "testslot_toast_value"
			pg.DBTables = []string{"test_toast_table"}
		}),
	)
	srcConn, err := postgres.MakeConnPoolFromSrc(&source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	ctx := context.Background()

	_, err = srcConn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_toast_table (
			id INT PRIMARY KEY,
			val jsonb,
			n int
		);
	`)
	require.NoError(t, err)

	_, err = srcConn.Exec(ctx, `
		ALTER TABLE public.test_toast_table REPLICA IDENTITY FULL;
	`)
	require.NoError(t, err)

	sink := &mocksink.MockSink{}
	target := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sink },
		Cleanup:       model.Drop,
	}

	counter := 0
	pushedItems := make([]abstract.ChangeItem, 0)
	sink.PushCallback = func(items []abstract.ChangeItem) error {
		for _, item := range items {
			if item.Kind == abstract.UpdateKind && item.Table == "test_toast_table" {
				logger.Log.Infof("QQQ::Pushed update item: %+v", item.ToJSONString())
				counter++
				require.Len(t, item.ColumnValues, 3)
				require.Len(t, item.ColumnNames, 3)
				require.Equal(t, item.OldKeys.KeyNames, []string{"id", "val", "n"})
				require.Len(t, item.OldKeys.KeyValues, 3)
				pushedItems = append(pushedItems, item)
			}
		}
		return nil
	}

	transfer := helpers.MakeTransfer("test_toast_value", &source, &target, abstract.TransferTypeIncrementOnly)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	var relreplident string
	err = srcConn.QueryRow(ctx, `
		SELECT relreplident::text
		FROM pg_class
		WHERE oid = 'test_toast_table'::regclass;
	`).Scan(&relreplident)
	require.NoError(t, err)
	require.Equal(t, "f", relreplident, "Table must have REPLICA IDENTITY FULL")

	t.Run("Update with toast value", func(t *testing.T) {
		insertToastValue(t, srcConn, 7)

		_, err = srcConn.Exec(ctx, `
			UPDATE test_toast_table SET n = 17 WHERE id = 7;
		`)
		require.NoError(t, err)
		waitForUpdate(t, &counter, 1, 30*time.Second)
		require.Equal(t, []interface{}{int32(7), int32(17), strings.Repeat("X", 500000)}, pushedItems[0].ColumnValues)
		pushedItems = pushedItems[:0]
	})

	t.Run("Update with null toast value", func(t *testing.T) {
		insertToastValue(t, srcConn, 8)
		_, err = srcConn.Exec(ctx, `
			UPDATE test_toast_table SET val = null WHERE id = 8;
		`)
		require.NoError(t, err)
		waitForUpdate(t, &counter, 2, 30*time.Second)
		require.Equal(t, []interface{}{int32(8), nil, int32(8)}, pushedItems[0].ColumnValues)
		pushedItems = pushedItems[:0]
	})
}
