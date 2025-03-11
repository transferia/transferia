package snapshot

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	"github.com/transferria/transferria/pkg/worker/tasks"
	"github.com/transferria/transferria/tests/helpers"
	"golang.org/x/net/context"
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
	SourceA = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix(""), pgrecipe.WithDBTables(descsToPgNames(tablesA)...), pgrecipe.WithEdit(func(pg *postgres.PgSource) {
		pg.User = "blockeduser"
		pg.Password = "sim-sim@OPEN"
	}))
	SourceIAForDump = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix(""), pgrecipe.WithDBTables(descsToPgNames(tablesIA)...))
	SourceIA        = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix(""), pgrecipe.WithDBTables(descsToPgNames(tablesIA)...), pgrecipe.WithEdit(func(pg *postgres.PgSource) {
		pg.User = "blockeduser"
		pg.Password = "sim-sim@OPEN"
	}))
	Target = *pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
)

var (
	sourceATID         = helpers.TransferID + "A"
	sourceIATID        = helpers.TransferID + "IA"
	sourceIAForDumpTID = helpers.TransferID + "IAForDump"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga

	Target.Cleanup = model.DisabledCleanup
	helpers.InitSrcDst(sourceATID, &SourceA, &Target, abstract.TransferTypeSnapshotOnly)
	helpers.InitSrcDst(sourceIATID, &SourceIA, &Target, abstract.TransferTypeSnapshotOnly)
	helpers.InitSrcDst(sourceIAForDumpTID, &SourceIAForDump, &Target, abstract.TransferTypeSnapshotOnly)
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source A", Port: SourceA.Port},
			helpers.LabeledPort{Label: "PG source IA for dump", Port: SourceIAForDump.Port},
			helpers.LabeledPort{Label: "PG source IA", Port: SourceIA.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	t.Run("Upload_accessible", UploadTestAccessible)
	t.Run("Upload_inaccessible", UploadTestInaccessible)
}

func UploadTestAccessible(t *testing.T) {
	transfer := helpers.MakeTransfer(sourceATID, &SourceA, &Target, abstract.TransferTypeSnapshotOnly)

	pgdump, err := postgres.ExtractPgDumpSchema(transfer)
	require.NoError(t, err)
	require.NoError(t, postgres.ApplyPgDumpPreSteps(pgdump, transfer, helpers.EmptyRegistry()))

	require.NoError(t, tasks.Upload(context.TODO(), coordinator.NewFakeClient(), *transfer, nil, tasks.UploadSpec{Tables: tablesA}, helpers.EmptyRegistry()))
}

func UploadTestInaccessible(t *testing.T) {
	transferForDump := helpers.MakeTransfer(sourceIAForDumpTID, &SourceIAForDump, &Target, abstract.TransferTypeSnapshotOnly)
	pgdump, err := postgres.ExtractPgDumpSchema(transferForDump)
	require.NoError(t, err)
	require.NoError(t, postgres.ApplyPgDumpPreSteps(pgdump, transferForDump, helpers.EmptyRegistry()))

	transfer := helpers.MakeTransfer(sourceIATID, &SourceIA, &Target, abstract.TransferTypeSnapshotOnly)
	err = tasks.Upload(context.TODO(), coordinator.NewFakeClient(), *transfer, nil, tasks.UploadSpec{Tables: tablesIA}, helpers.EmptyRegistry())
	require.Error(t, err)
	require.Contains(t, err.Error(), "Missing tables in source (pg)")
	require.Contains(t, err.Error(), `"public"."t_inaccessible"`)
}
