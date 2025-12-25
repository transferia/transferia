package toast

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	pg_provider "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	Source   = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"))
	Target   = *pgrecipe.RecipeTarget(pgrecipe.WithInitDir("init_target"))
	ErrRetry = xerrors.NewSentinel("Retry")
)

func init() {
	_ = os.Setenv("YC", "1")                                                                     // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, abstract.TransferTypeIncrementOnly) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func largeString(n int, s string) string {
	var result string
	for i := 0; i < n; i++ {
		result += s
	}
	return result
}

func makeTestFunction(usePolling bool) func(t *testing.T) {
	var schema, slotID string
	if usePolling {
		schema = "s1"
		slotID = "slot1"
	} else {
		schema = "s2"
		slotID = "slot2"
	}

	return func(t *testing.T) {
		sourceCopy := Source
		sourceCopy.UsePolling = usePolling
		sourceCopy.SlotID = slotID
		sourceCopy.KeeperSchema = schema
		sourceCopy.DBTables = []string{fmt.Sprintf("%s.__test", schema)}
		transfer := model.Transfer{
			ID:  "test_id",
			Src: &sourceCopy,
			Dst: &Target,
		}

		srcConn, err := pg_provider.MakeConnPoolFromSrc(&Source, logger.Log)
		require.NoError(t, err)
		defer srcConn.Close()
		dstConn, err := pg_provider.MakeConnPoolFromDst(&Target, logger.Log)
		require.NoError(t, err)
		defer dstConn.Close()

		defer func() {
			r, err := srcConn.Exec(context.Background(), fmt.Sprintf(`DELETE FROM %s.__test`, schema))
			require.NoError(t, err)
			require.EqualValues(t, 2, r.RowsAffected())
			r, err = dstConn.Exec(context.Background(), fmt.Sprintf(`DELETE FROM %s.__test`, schema))
			require.NoError(t, err)
			require.EqualValues(t, 2, r.RowsAffected())
		}()

		worker := local.NewLocalWorker(coordinator.NewFakeClient(), &transfer, helpers.EmptyRegistry(), logger.Log)
		worker.Start()
		defer worker.Stop() //nolint

		// 1. Insert two rows, a small and a big one
		_, err = srcConn.Exec(context.Background(), fmt.Sprintf(`INSERT INTO %s.__test VALUES (1, 10, 'a')`, schema))
		require.NoError(t, err)
		_, err = srcConn.Exec(context.Background(), fmt.Sprintf(`INSERT INTO %s.__test VALUES (2, 20, $1)`, schema), largeString(16384, "a"))
		require.NoError(t, err)

		var small int
		var large string
		err = backoff.Retry(func() error {
			err := dstConn.QueryRow(context.Background(), fmt.Sprintf(`SELECT small, large FROM %s.__test WHERE id = 1`, schema)).Scan(&small, &large)
			if err != nil {
				if !xerrors.Is(err, pgx.ErrNoRows) {
					return backoff.Permanent(err)
				}
				logger.Log.Warnf("select err: %v", err)
			}
			return err
		}, backoff.NewConstantBackOff(time.Second))
		require.NoError(t, err)
		require.Equal(t, 10, small)
		require.Equal(t, "a", large)

		err = backoff.Retry(func() error {
			err = dstConn.QueryRow(context.Background(), fmt.Sprintf(`SELECT small, large FROM %s.__test WHERE id = 2`, schema)).Scan(&small, &large)
			if err != nil {
				if !xerrors.Is(err, pgx.ErrNoRows) {
					return backoff.Permanent(err)
				}
				logger.Log.Warnf("select err: %v", err)
			}
			return err
		}, backoff.NewConstantBackOff(time.Second))
		require.NoError(t, err)
		require.Equal(t, 20, small)
		require.Equal(t, largeString(16384, "a"), large)

		// 2. Modify both rows
		r, err := srcConn.Exec(context.Background(), fmt.Sprintf(`UPDATE %s.__test SET small = 30`, schema))
		require.NoError(t, err)
		require.EqualValues(t, 2, r.RowsAffected())
		r, err = srcConn.Exec(context.Background(), fmt.Sprintf(`UPDATE %s.__test SET large = 'b' WHERE id = 1`, schema))
		require.NoError(t, err)
		require.EqualValues(t, 1, r.RowsAffected())

		err = backoff.Retry(func() error {
			err = dstConn.QueryRow(context.Background(), fmt.Sprintf(`SELECT small, large FROM %s.__test WHERE id = 1`, schema)).Scan(&small, &large)
			require.NoError(t, err)
			if small != 30 {
				logger.Log.Warnf(`Unexpected "small" value: %d`, small)
				return ErrRetry
			}
			if large != "b" {
				logger.Log.Warnf(`Unexpected "large" value: %s`, large)
				return ErrRetry
			}

			err = dstConn.QueryRow(context.Background(), fmt.Sprintf(`SELECT small, large FROM %s.__test WHERE id = 2`, schema)).Scan(&small, &large)
			require.NoError(t, err)
			if small != 30 {
				logger.Log.Warnf(`Unexpected "small" value: %d`, small)
				return ErrRetry
			}
			if large != largeString(16384, "a") {
				logger.Log.Warnf(`Unexpected "large" value: %s`, large)
				return ErrRetry
			}
			return nil
		}, backoff.NewConstantBackOff(time.Second))
	}
}

func TestToast(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	t.Run("TestToast/UsePollingFalse", makeTestFunction(false))
	t.Run("TestToast/UsePollingTrue", makeTestFunction(true))
}
