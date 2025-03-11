package multiindex

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/abstract/model"
	pg_provider "github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	"github.com/transferria/transferria/pkg/runtime/local"
	"github.com/transferria/transferria/tests/helpers"
)

var (
	TransferType = abstract.TransferTypeIncrementOnly
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"), pgrecipe.WithDBTables("public.test"))
	Target       = *pgrecipe.RecipeTarget(pgrecipe.WithInitDir("init_target"))
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestMultiindexBasic(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)

	srcConn, err := pg_provider.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()
	dstConn, err := pg_provider.MakeConnPoolFromDst(&Target, logger.Log)
	require.NoError(t, err)
	defer dstConn.Close()

	_, err = srcConn.Exec(context.Background(), `SELECT pg_create_logical_replication_slot('testslot', 'wal2json')`)
	require.NoError(t, err)
	defer func() {
		_, err := srcConn.Exec(context.Background(), `SELECT pg_drop_replication_slot('testslot')`)
		require.NoError(t, err)
	}()

	_, err = srcConn.Exec(context.Background(), `
		INSERT INTO test VALUES (1, 777, 'a'); -- {1: (777, 'a')}
		DELETE FROM test WHERE aid = 1;        -- {}
		INSERT INTO test VALUES (2, 777, 'b'); -- {2: (777, 'b')}
		-- Target database is here
		INSERT INTO test VALUES (3, 888, 'c'); -- {2: (777, 'b'), 3: (888, 'c')}
	`)
	require.NoError(t, err)
	defer func() {
		_, err := srcConn.Exec(context.Background(), `DELETE FROM test;`)
		require.NoError(t, err)
	}()

	_, err = dstConn.Exec(context.Background(), `INSERT INTO test VALUES (2, 777, 'b')`)
	require.NoError(t, err)
	defer func() {
		_, err := dstConn.Exec(context.Background(), `DELETE FROM test;`)
		require.NoError(t, err)
	}()

	worker := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)
	worker.Start()
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
	err = worker.Stop()
	require.NoError(t, err)

	var aid, bid int
	var value string
	err = dstConn.QueryRow(context.Background(), `SELECT aid, bid, value FROM test WHERE aid = 2`).Scan(&aid, &bid, &value)
	require.NoError(t, err)
	require.Equal(t, 2, aid)
	require.Equal(t, 777, bid)
	require.Equal(t, "b", value)

	err = dstConn.QueryRow(context.Background(), `SELECT aid, bid, value FROM test WHERE aid = 3`).Scan(&aid, &bid, &value)
	require.NoError(t, err)
	require.Equal(t, 3, aid)
	require.Equal(t, 888, bid)
	require.Equal(t, "c", value)
}

func TestMultiindexPkeyChange(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	Target.PerTransactionPush = true // in per table mode result depends on collapse and so may flap

	transfer := model.Transfer{
		ID:  "test_id",
		Src: &Source,
		Dst: &Target,
	}

	srcConn, err := pg_provider.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()
	dstConn, err := pg_provider.MakeConnPoolFromDst(&Target, logger.Log)
	require.NoError(t, err)
	defer dstConn.Close()

	_, err = srcConn.Exec(context.Background(), `SELECT pg_create_logical_replication_slot('testslot', 'wal2json')`)
	require.NoError(t, err)
	defer func() {
		_, err := srcConn.Exec(context.Background(), `SELECT pg_drop_replication_slot('testslot')`)
		require.NoError(t, err)
	}()

	_, err = srcConn.Exec(context.Background(), `
		INSERT INTO test VALUES (1, 777, 'a');                    -- {1: (777, 'a')}
		UPDATE test SET aid = 2, bid = 888 WHERE aid = 1;         -- {2: (888, 'a')}
		UPDATE test SET bid = 999 WHERE aid = 2;                  -- {2: (999, 'a')}
		INSERT INTO test VALUES (3, 888, 'b');                    -- {2: (999, 'a'), 3: (888, 'b')}
		-- Target database is here
	`)
	require.NoError(t, err)
	defer func() {
		_, err := srcConn.Exec(context.Background(), `DELETE FROM test;`)
		require.NoError(t, err)
	}()

	_, err = dstConn.Exec(context.Background(), `
		INSERT INTO test VALUES (2, 999, 'a');
		INSERT INTO test VALUES (3, 888, 'b');
	`)
	require.NoError(t, err)
	defer func() {
		_, err := dstConn.Exec(context.Background(), `DELETE FROM test;`)
		require.NoError(t, err)
	}()

	worker := local.NewLocalWorker(coordinator.NewFakeClient(), &transfer, helpers.EmptyRegistry(), logger.Log)
	err = worker.Run()
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "duplicate key value violates unique constraint")
	err = worker.Stop()
	require.NoError(t, err)
}
