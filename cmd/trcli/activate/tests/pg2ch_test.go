package tests

import (
	_ "embed"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/cmd/trcli/activate"
	"github.com/transferia/transferia/cmd/trcli/config"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	chrecipe "github.com/transferia/transferia/pkg/providers/clickhouse/recipe"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

//go:embed transfer.yaml
var transferYaml []byte

func TestActivate(t *testing.T) {
	src := pgrecipe.RecipeSource(
		pgrecipe.WithPrefix(""),
		pgrecipe.WithFiles("dump/pg_init.sql"),
	)

	dst, err := chrecipe.Target(
		chrecipe.WithInitFile("ch_init.sql"),
		chrecipe.WithDatabase("trcli_activate_test_ch"),
	)
	require.NoError(t, err)

	transfer, err := config.ParseTransfer(transferYaml)
	require.NoError(t, err)

	transfer.Src = src
	transfer.Dst = dst

	require.NoError(t, activate.RunActivate(coordinator.NewStatefulFakeClient(), transfer, solomon.NewRegistry(solomon.NewRegistryOpts()), 0))

	require.NoError(t, helpers.WaitDestinationEqualRowsCount(dst.Database, "t2", helpers.GetSampleableStorageByModel(t, dst), 60*time.Second, 2))
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(dst.Database, "t3", helpers.GetSampleableStorageByModel(t, dst), 60*time.Second, 5))
}

func TestActivateWithDelay(t *testing.T) {
	src := pgrecipe.RecipeSource(
		pgrecipe.WithPrefix(""),
		pgrecipe.WithFiles("dump/pg_init.sql"),
	)

	dst, err := chrecipe.Target(
		chrecipe.WithInitFile("ch_init.sql"),
		chrecipe.WithDatabase("trcli_activate_test_ch"),
	)
	require.NoError(t, err)

	transfer, err := config.ParseTransfer(transferYaml)
	require.NoError(t, err)

	transfer.Src = src
	transfer.Dst = dst

	st := time.Now()
	require.NoError(t, activate.RunActivate(coordinator.NewStatefulFakeClient(), transfer, solomon.NewRegistry(solomon.NewRegistryOpts()), 10*time.Second))
	require.Less(t, 10*time.Second, time.Since(st))
}
