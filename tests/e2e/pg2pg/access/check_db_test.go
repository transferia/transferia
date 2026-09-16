package snapshot

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	tablesA = []abstract.TableDescription{
		{
			Schema: "public",
			Name:   "t_accessible",
			Filter: abstract.NoFilter,
			EtaRow: 0,
			Offset: 0,
		},
		{
			Schema: "public",
			Name:   "t_empty",
			Filter: abstract.NoFilter,
			EtaRow: 0,
			Offset: 0,
		},
	}
	tablesIA = []abstract.TableDescription{
		{
			Schema: "public",
			Name:   "t_inaccessible",
			Filter: abstract.NoFilter,
			EtaRow: 0,
			Offset: 0,
		},
		{
			Schema: "public",
			Name:   "t_empty",
			Filter: abstract.NoFilter,
			EtaRow: 0,
			Offset: 0,
		},
	}
)

func descsToPgNames(descs []abstract.TableDescription) []string {
	result := make([]string, 0)
	for _, d := range descs {
		result = append(result, d.Fqtn())
	}
	return result
}

var (
	SourceA = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix(""), pgrecipe.WithDBTables(descsToPgNames(tablesA)...), pgrecipe.WithEdit(func(pg *provider_postgres.PgSource) {
		pg.User = "blockeduser"
		pg.Password = "sim-sim@OPEN"
	}))
	SourceIAForDump = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix(""), pgrecipe.WithDBTables(descsToPgNames(tablesIA)...))
	SourceIA        = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix(""), pgrecipe.WithDBTables(descsToPgNames(tablesIA)...), pgrecipe.WithEdit(func(pg *provider_postgres.PgSource) {
		pg.User = "blockeduser"
		pg.Password = "sim-sim@OPEN"
	}))
	Target = *pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
)

var (
	sourceATID         = transferhelpers.TransferID + "A"
	sourceIATID        = transferhelpers.TransferID + "IA"
	sourceIAForDumpTID = transferhelpers.TransferID + "IAForDump"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga

	Target.Cleanup = model.DisabledCleanup
	transferhelpers.InitSrcDst(sourceATID, &SourceA, &Target, abstract.TransferTypeSnapshotOnly)
	transferhelpers.InitSrcDst(sourceIATID, &SourceIA, &Target, abstract.TransferTypeSnapshotOnly)
	transferhelpers.InitSrcDst(sourceIAForDumpTID, &SourceIAForDump, &Target, abstract.TransferTypeSnapshotOnly)
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source A", Port: SourceA.Port},
			network.LabeledPort{Label: "PG source IA for dump", Port: SourceIAForDump.Port},
			network.LabeledPort{Label: "PG source IA", Port: SourceIA.Port},
			network.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	t.Run("Upload_accessible", UploadTestAccessible)
	t.Run("Upload_inaccessible", UploadTestInaccessible)
}

func UploadTestAccessible(t *testing.T) {
	transfer := transferhelpers.MakeTransfer(sourceATID, &SourceA, &Target, abstract.TransferTypeSnapshotOnly)

	pgdump, err := provider_postgres.ExtractPgDumpSchema(transfer)
	require.NoError(t, err)
	require.NoError(t, provider_postgres.ApplyPgDumpPreSteps(pgdump, transfer, &model.TransferOperation{}, testmetrics.EmptyRegistry()))

	require.NoError(t, tasks.Upload(context.TODO(), coordinator.NewFakeClient(), *transfer, nil, tasks.UploadSpec{Tables: tablesA}, testmetrics.EmptyRegistry()))
}

func UploadTestInaccessible(t *testing.T) {
	transferForDump := transferhelpers.MakeTransfer(sourceIAForDumpTID, &SourceIAForDump, &Target, abstract.TransferTypeSnapshotOnly)
	pgdump, err := provider_postgres.ExtractPgDumpSchema(transferForDump)
	require.NoError(t, err)
	require.NoError(t, provider_postgres.ApplyPgDumpPreSteps(pgdump, transferForDump, &model.TransferOperation{}, testmetrics.EmptyRegistry()))

	transfer := transferhelpers.MakeTransfer(sourceIATID, &SourceIA, &Target, abstract.TransferTypeSnapshotOnly)
	err = tasks.Upload(context.TODO(), coordinator.NewFakeClient(), *transfer, nil, tasks.UploadSpec{Tables: tablesIA}, testmetrics.EmptyRegistry())
	require.Error(t, err)
	require.Contains(t, err.Error(), "Missing tables in source (pg)")
	require.Contains(t, err.Error(), `"public"."t_inaccessible"`)
}
