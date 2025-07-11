package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	pgcommon "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/tests/helpers"
	"go.ytsaurus.tech/library/go/core/log"
)

//---------------------------------------------------------------------------------------------------------------------
// mockSinker

type mockSinker struct {
	pushCallback func([]abstract.ChangeItem) error
}

func (s *mockSinker) Close() error {
	return nil
}

func (s *mockSinker) Push(input []abstract.ChangeItem) error {
	return s.pushCallback(input)
}

//---------------------------------------------------------------------------------------------------------------------

func TestReplication(t *testing.T) {
	t.Setenv("YC", "1") // to not go to vanga

	sinker := &mockSinker{}
	wg := sync.WaitGroup{}
	maxIter := 5
	wg.Add(maxIter)
	iter := 0
	source := pgrecipe.RecipeSource(
		pgrecipe.WithInitDir("init_source"),
		pgrecipe.WithEdit(func(pg *pgcommon.PgSource) {
			pg.DBTables = []string{"public.__test1"}
		}),
	)
	sinker.pushCallback = func(input []abstract.ChangeItem) error {
		if iter < maxIter {
			wg.Done()
			iter++
		}
		logger.Log.Infof("push will return error to trigger retry: %v", iter)
		return xerrors.New("synthetic error")
	}
	transfer := &model.Transfer{
		ID:  "test_id",
		Src: source,
		Dst: &model.MockDestination{
			SinkerFactory: func() abstract.Sinker {
				return sinker
			},
			Cleanup: model.DisabledCleanup,
		},
		Type: abstract.TransferTypeIncrementOnly,
	}

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	ctx := context.Background()
	srcConn, err := pgcommon.MakeConnPoolFromSrc(source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	_, err = srcConn.Exec(ctx, `insert into __test1 (id, value) values (1, 'test');`) //nolint
	require.NoError(t, err)

	wg.Wait()

	logger.Log.Info("pusher retries done")
	storage := helpers.GetSampleableStorageByModel(t, transfer.Src)
	pgStorage, ok := storage.(*pgcommon.Storage)
	require.True(t, ok)

	logger.Log.Info("local worker stop")
	// wait all connection closed
	require.NoError(
		t,
		backoff.RetryNotify(
			func() error {
				rows, err := pgStorage.Conn.Query(context.Background(), "select * from pg_stat_activity where query NOT ILIKE '%pg_stat_activity%' and backend_type = 'client backend'")
				if err != nil {
					return err
				}
				var connections []map[string]interface{}
				for rows.Next() {
					vals, err := rows.Values()
					if err != nil {
						return err
					}
					row := map[string]interface{}{}
					for i, f := range rows.FieldDescriptions() {
						row[string(f.Name)] = vals[i]
					}
					connections = append(connections, row)
				}
				if rows.Err() != nil {
					return rows.Err()
				}
				if len(connections) < 5 {
					return nil
				}
				logger.Log.Warn("too many connections", log.Any("connections", connections))
				return xerrors.Errorf("connection exceeded limit: %v > 5", len(connections))
			},
			backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Second), 20),
			util.BackoffLogger(logger.Log, "check connection count"),
		),
	)
}
