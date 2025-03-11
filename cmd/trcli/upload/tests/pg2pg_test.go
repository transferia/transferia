package tests

import (
	_ "embed"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/cmd/trcli/config"
	"github.com/transferia/transferia/cmd/trcli/upload"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	chrecipe "github.com/transferia/transferia/pkg/providers/clickhouse/recipe"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

//go:embed transfer.yaml
var transferYaml []byte

//go:embed tables.yaml
var tablesYaml []byte

func TestUpload(t *testing.T) {
	src := pgrecipe.RecipeSource(
		pgrecipe.WithPrefix(""),
		pgrecipe.WithFiles("dump/pg_init.sql"),
	)

	dst, err := chrecipe.Target(
		chrecipe.WithInitFile("ch_init.sql"),
		chrecipe.WithDatabase("trcli_upload_test_ch"),
	)
	require.NoError(t, err)

	transfer, err := config.ParseTransfer(transferYaml)
	require.NoError(t, err)

	transfer.Src = src
	transfer.Dst = dst

	tables, err := config.ParseTablesYaml(tablesYaml)
	require.NoError(t, err)

	require.NoError(t, upload.RunUpload(coordinator.NewFakeClient(), transfer, tables, solomon.NewRegistry(solomon.NewRegistryOpts())))
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(dst.Database, "t2", helpers.GetSampleableStorageByModel(t, dst), 60*time.Second, 2))
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(dst.Database, "t3", helpers.GetSampleableStorageByModel(t, dst), 60*time.Second, 2))
}
