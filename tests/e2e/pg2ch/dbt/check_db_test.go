package dbt

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/library/go/test/yatest"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	chrecipe "github.com/transferria/transferria/pkg/providers/clickhouse/recipe"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	"github.com/transferria/transferria/pkg/runtime/shared/pod"
	transformers_registry "github.com/transferria/transferria/pkg/transformer"
	"github.com/transferria/transferria/pkg/transformer/registry/dbt"
	_ "github.com/transferria/transferria/pkg/transformer/registry/dbt/clickhouse"
	"github.com/transferria/transferria/tests/helpers"
)

func TestSnapshot(t *testing.T) {
	t.Skip()
	t.Setenv("DBT_CONTAINER_REGISTRY", "12197361.preprod")
	t.Setenv("DBT_IMAGE_TAG", "public.ecr.aws/t9p9v8b9")

	source := pgrecipe.RecipeSource(
		pgrecipe.WithInitFiles(yatest.SourcePath("transfer_manager/go/tests/e2e/pg2ch/dbt/init_pg.sql")),
		pgrecipe.WithoutPgDump(),
	)
	target := chrecipe.MustTarget(
		chrecipe.WithInitFile(yatest.SourcePath("transfer_manager/go/tests/e2e/pg2ch/dbt/init_ch.sql")),
		chrecipe.WithDatabase("dbttest"),
	)

	pod.SharedDir = "/tmp"

	githubPAT := os.Getenv("DOUBLECLOUD_GITHUB_PERSONAL_ACCESS_TOKEN")
	if githubPAT == "" {
		t.Skip("DOUBLECLOUD_GITHUB_PERSONAL_ACCESS_TOKEN not provided")
	}
	require.NotEmpty(t, githubPAT)

	// Source.WithDefaults() // has already been initialized by the `helpers` package
	target.WithDefaults()
	target.ProtocolUnspecified = true
	target.UseSchemaInTableName = true
	target.Cleanup = model.Drop
	transfer := helpers.MakeTransfer("testtransfer", source, target, abstract.TransferTypeSnapshotOnly)
	addTransformationToTransfer(transfer, dbt.Config{
		GitRepositoryLink: fmt.Sprintf("https://%s@github.com/doublecloud/tests-clickhouse-dbt.git", githubPAT),
		ProfileName:       "clickhouse",
		Operation:         "run",
	})

	_ = helpers.Activate(t, transfer)

	targetAsStorage := helpers.GetSampleableStorageByModel(t, target)
	targetTables, err := targetAsStorage.TableList(nil)
	require.NoError(t, err)
	require.Contains(t, targetTables, *abstract.NewTableID("dbttest", "v1"))
	require.Contains(t, targetTables, *abstract.NewTableID("dbttest", "v2"))
	require.Contains(t, targetTables, *abstract.NewTableID("dbttest", "v3"))
}

func addTransformationToTransfer(transfer *model.Transfer, config dbt.Config) {
	if transfer.Transformation == nil {
		transfer.Transformation = &model.Transformation{
			ExtraTransformers: nil,
			Executor:          nil,
		}
	}
	if transfer.Transformation.Transformers == nil {
		transfer.Transformation.Transformers = new(transformers_registry.Transformers)
	}
	transfer.Transformation.Transformers.Transformers = append(transfer.Transformation.Transformers.Transformers, transformers_registry.Transformer{
		dbt.TransformerType: config,
	})
}
