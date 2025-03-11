package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/abstract/model"
	pg_provider "github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	"github.com/transferria/transferria/pkg/runtime/local"
	"github.com/transferria/transferria/pkg/util"
	"github.com/transferria/transferria/tests/helpers"
	"go.ytsaurus.tech/library/go/core/log"
)

type mockSinker struct {
	pushCallback func([]abstract.ChangeItem) error
}

func (s *mockSinker) Close() error {
	return nil
}

func (s *mockSinker) Push(input []abstract.ChangeItem) error {
	return s.pushCallback(input)
}

func TestReplication(t *testing.T) {
	t.Setenv("YC", "1") // to not go to vanga
	sinker := &mockSinker{}
	transfer := model.Transfer{
		ID: "test_id",
		Src: pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"), pgrecipe.WithEdit(func(pg *pg_provider.PgSource) {
			pg.DBTables = []string{"public.__test1"}
		})),
		Dst: &model.MockDestination{SinkerFactory: func() abstract.Sinker {
			return sinker
		}},
	}

	wg := sync.WaitGroup{}
	maxIter := 5
	wg.Add(maxIter)
	iter := 0
	sinker.pushCallback = func(input []abstract.ChangeItem) error {
		if iter < maxIter {
			wg.Done()
			iter++
		}
		logger.Log.Infof("push will return error to trigger retry: %v", iter)
		return xerrors.New("synthetic error")
	}

	localWorker := local.NewLocalWorker(coordinator.NewFakeClient(), &transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	wg.Wait()
	logger.Log.Info("pusher retries done")
	storage := helpers.GetSampleableStorageByModel(t, transfer.Src)
	pgStorage, ok := storage.(*pg_provider.Storage)
	require.True(t, ok)
	logger.Log.Info("local worker stop")
	// wait all connection closed
	require.NoError(t,
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
		))
	require.NoError(t, localWorker.Stop())
}
