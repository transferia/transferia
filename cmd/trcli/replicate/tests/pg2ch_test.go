package tests

import (
	"context"
	_ "embed"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/cmd/trcli/config"
	"github.com/transferria/transferria/cmd/trcli/replicate"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/metrics/solomon"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	chrecipe "github.com/transferria/transferria/pkg/providers/clickhouse/recipe"
	pgcommon "github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	"github.com/transferria/transferria/tests/helpers"
)

//go:embed transfer.yaml
var transferYaml []byte

func TestReplicate(t *testing.T) {
	src := pgrecipe.RecipeSource(
		pgrecipe.WithPrefix(""),
		pgrecipe.WithFiles("dump/pg_init.sql"),
	)

	dst, err := chrecipe.Target(
		chrecipe.WithInitFile("ch_init.sql"),
		chrecipe.WithDatabase("trcli_replicate_test_ch"),
	)
	require.NoError(t, err)
	transfer, err := config.ParseTransfer(transferYaml)
	require.NoError(t, err)

	src.SlotID = transfer.ID
	transfer.Src = src
	transfer.Dst = dst

	go func() {
		require.NoError(t, replicate.RunReplication(coordinator.NewStatefulFakeClient(), transfer, solomon.NewRegistry(solomon.NewRegistryOpts())))
	}()

	time.Sleep(5 * time.Second)

	connConfig, err := pgcommon.MakeConnConfigFromSrc(logger.Log, src)
	require.NoError(t, err)

	conn, err := pgcommon.NewPgConnPool(connConfig, logger.Log)
	require.NoError(t, err)

	rows, err := conn.Query(context.Background(), "INSERT INTO public.t2(i, f) VALUES (3, 1.0), (4, 4.0)")
	require.NoError(t, err)
	rows.Close()

	rows, err = conn.Query(context.Background(), "INSERT INTO public.t3(i, f) VALUES (1, 2.0), (2, 3.0)")
	require.NoError(t, err)
	rows.Close()

	require.NoError(t, helpers.WaitDestinationEqualRowsCount(dst.Database, "t2", helpers.GetSampleableStorageByModel(t, dst), 60*time.Second, 2))
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(dst.Database, "t3", helpers.GetSampleableStorageByModel(t, dst), 60*time.Second, 2))
}
