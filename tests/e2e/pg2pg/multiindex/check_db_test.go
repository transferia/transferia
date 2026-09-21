package multiindex

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	TransferType = abstract.TransferTypeIncrementOnly

	SourceBasic = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"), pgrecipe.WithDBTables("public.test_basic"), pgrecipe.WithEdit(func(pg *provider_postgres.PgSource) {
		pg.PreSteps = &provider_postgres.PgDumpSteps{}
	}))
	TargetBasic = *pgrecipe.RecipeTarget(pgrecipe.WithInitDir("init_target"))

	SourceChangePkey = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"), pgrecipe.WithDBTables("public.test_change_pkey"), pgrecipe.WithEdit(func(pg *provider_postgres.PgSource) {
		pg.PreSteps = &provider_postgres.PgDumpSteps{}
	}))
	TargetChangePkey = *pgrecipe.RecipeTarget(pgrecipe.WithInitDir("init_target"))
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

func TestMultiindexBasic(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: SourceBasic.Port},
			network.LabeledPort{Label: "PG target", Port: TargetBasic.Port},
		))
	}()

	transferID := transferhelpers.GenerateTransferID("TestMultiindexBasic")
	transferhelpers.InitSrcDst(transferID, &SourceBasic, &TargetBasic, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
	transfer := transferhelpers.MakeTransfer(transferID, &SourceBasic, &TargetBasic, TransferType)

	srcConn, err := provider_postgres.MakeConnPoolFromSrc(&SourceBasic, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()
	dstConn, err := provider_postgres.MakeConnPoolFromDst(&TargetBasic, logger.Log)
	require.NoError(t, err)
	defer dstConn.Close()

	// activate
	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	// insert data
	_, err = srcConn.Exec(context.Background(), `
		INSERT INTO test_basic VALUES (1, 777, 'a'); -- {1: (777, 'a')}
		DELETE FROM test_basic WHERE aid = 1;        -- {}
		INSERT INTO test_basic VALUES (2, 777, 'b'); -- {2: (777, 'b')}
		-- Target database is here
		INSERT INTO test_basic VALUES (3, 888, 'c'); -- {2: (777, 'b'), 3: (888, 'c')}
	`)
	require.NoError(t, err)

	// wait
	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "test_basic", storagecomparison.GetSampleableStorageByModel(t, SourceBasic), storagecomparison.GetSampleableStorageByModel(t, TargetBasic), 60*time.Second))

	// check
	var aid, bid int
	var value string
	err = dstConn.QueryRow(context.Background(), `SELECT aid, bid, value FROM test_basic WHERE aid = 2`).Scan(&aid, &bid, &value)
	require.NoError(t, err)
	require.Equal(t, 2, aid)
	require.Equal(t, 777, bid)
	require.Equal(t, "b", value)

	err = dstConn.QueryRow(context.Background(), `SELECT aid, bid, value FROM test_basic WHERE aid = 3`).Scan(&aid, &bid, &value)
	require.NoError(t, err)
	require.Equal(t, 3, aid)
	require.Equal(t, 888, bid)
	require.Equal(t, "c", value)
}

func TestMultiindexPkeyChange(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: SourceChangePkey.Port},
			network.LabeledPort{Label: "PG target", Port: TargetChangePkey.Port},
		))
	}()

	TargetChangePkey.PerTransactionPush = true // in per table mode result depends on collapse and so may flap

	transferID := transferhelpers.GenerateTransferID("TestMultiindexPkeyChange")
	transferhelpers.InitSrcDst(transferID, &SourceChangePkey, &TargetChangePkey, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
	transfer := transferhelpers.MakeTransfer(transferID, &SourceChangePkey, &TargetChangePkey, TransferType)

	srcConn, err := provider_postgres.MakeConnPoolFromSrc(&SourceChangePkey, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()
	dstConn, err := provider_postgres.MakeConnPoolFromDst(&TargetChangePkey, logger.Log)
	require.NoError(t, err)
	defer dstConn.Close()

	// insert into dst

	_, err = dstConn.Exec(context.Background(), `
		INSERT INTO test_change_pkey VALUES (2, 999, 'a');
		INSERT INTO test_change_pkey VALUES (3, 888, 'b');
		INSERT INTO test_change_pkey VALUES (4, 666, 'c');
	`)
	require.NoError(t, err)

	// activate
	worker := delivery.ActivateWithoutStart(t, transfer)

	// insert data
	_, err = srcConn.Exec(context.Background(), `
		INSERT INTO test_change_pkey VALUES (1, 777, 'a');                    -- {1: (777, 'a')}
		UPDATE test_change_pkey SET aid = 2, bid = 888 WHERE aid = 1;         -- {2: (888, 'a')}
		UPDATE test_change_pkey SET bid = 999 WHERE aid = 2;                  -- {2: (999, 'a')}
		INSERT INTO test_change_pkey VALUES (3, 888, 'b');                    -- {2: (999, 'a'), 3: (888, 'b')}
		-- Target database is here
	`)
	require.NoError(t, err)

	err = worker.Run()
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "duplicate key value violates unique constraint")
}
