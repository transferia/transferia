package pgdump

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/util/set"
	"github.com/transferia/transferia/tests/helpers"
	"github.com/transferia/transferia/tests/helpers/yatestx"
)

var (
	TransferType   = abstract.TransferTypeSnapshotOnly
	Source         = *pgrecipe.RecipeSource(pgrecipe.WithInitDir(yatestx.ProjectSource("dump")), pgrecipe.WithPrefix(""))
	Target         = *pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
	targetAsSource = postgres.PgSource{
		ClusterID:     Target.ClusterID,
		Hosts:         Target.Hosts,
		User:          Target.User,
		Password:      Target.Password,
		Database:      Target.Database,
		Port:          Target.Port,
		PgDumpCommand: Source.PgDumpCommand,
	}
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	require.True(t, t.Run("Existence", Existence))
	require.True(t, t.Run("Snapshot", Snapshot))
}

func Existence(t *testing.T) {
	_, err := postgres.NewStorage(Source.ToStorageParams(nil))
	require.NoError(t, err)
	_, err = postgres.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
}

func Snapshot(t *testing.T) {
	Source.PreSteps.Cast = true
	targetAsSource.WithDefaults()

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly)

	// extract schema
	itemsSource, err := postgres.ExtractPgDumpSchema(transfer)
	require.NoError(t, err)

	// apply on target
	require.NoError(t, postgres.ApplyPgDumpPreSteps(itemsSource, transfer, &model.TransferOperation{}, helpers.EmptyRegistry()))
	require.NoError(t, postgres.ApplyPgDumpPostSteps(itemsSource, transfer, &model.TransferOperation{}, helpers.EmptyRegistry()))

	// make target a source and extract its schema
	targetAsSource.PreSteps = Source.PreSteps
	targetAsSource.PostSteps = Source.PostSteps
	backwardFakeTransfer := helpers.MakeTransfer(helpers.TransferID, &targetAsSource, &Target, abstract.TransferTypeSnapshotOnly)
	itemsTarget, err := postgres.ExtractPgDumpSchema(backwardFakeTransfer)
	require.NoError(t, err)

	// compare schemas
	require.Less(t, 0, len(itemsSource))
	require.Equal(t, len(itemsSource), len(itemsTarget))
	require.Equal(t, itemsSource, itemsTarget)
	setvalsCount := 0
	for i := 0; i < len(itemsSource); i++ {
		require.Equal(t, itemsSource[i].Typ, itemsTarget[i].Typ)
		require.Equal(t, itemsSource[i].Body, itemsTarget[i].Body)
		if strings.Contains(itemsSource[i].Body, "setval(") {
			setvalsCount += 1
		}
	}
	require.Equal(t, 2, setvalsCount, "The number of setval() calls for SEQUENCEs must be equal to the number of sequences in dump")

	// test extract dump with DBTables
	// with custom types, also check cast, function, collation and index
	itemTypToCnt := extractPgDumpTypToCnt(t, []string{"santa.\"Ho-Ho-Ho\""}, []string{"santa"})
	require.Equal(t, 0, itemTypToCnt["POLICY"])
	require.Equal(t, 1, itemTypToCnt["CAST"])
	require.Equal(t, 2, itemTypToCnt["TYPE"])
	require.Equal(t, 1, itemTypToCnt["FUNCTION"])
	require.Equal(t, 0, itemTypToCnt["COLLATION"])
	require.Equal(t, 1, itemTypToCnt["INDEX"])

	// without custom types
	itemTypToCnt = extractPgDumpTypToCnt(t, []string{"public.__test"}, []string{"public"})
	require.Equal(t, 0, itemTypToCnt["TYPE"])
	require.Equal(t, 1, itemTypToCnt["POLICY"])
	require.Equal(t, 1, itemTypToCnt["FUNCTION"])
	require.Equal(t, 1, itemTypToCnt["COLLATION"])

	// transfer tables from public and santa schemas
	itemTypToCnt = extractPgDumpTypToCnt(t, []string{"public.__test", "santa.\"Ho-Ho-Ho\""}, []string{"public", "santa"})
	require.Equal(t, 2, itemTypToCnt["TYPE"])
	require.Equal(t, 3, itemTypToCnt["FUNCTION"])
	require.Equal(t, 1, itemTypToCnt["POLICY"])

	// tableAttach
	itemTypToCnt = extractPgDumpTypToCnt(t, []string{"public.wide_boys", "public.wide_boys_part_1", "public.wide_boys_part_2"}, []string{"public"})
	require.Equal(t, 2, itemTypToCnt["TABLE_ATTACH"])

	// without table attach
	itemTypToCnt = extractPgDumpTypToCnt(t, []string{"public.wide_boys_part_1"}, []string{"public"})
	require.Equal(t, 0, itemTypToCnt["TABLE_ATTACH"])

	// PRIMARY KEY, FK_CONSTRAINT
	itemTypToCnt = extractPgDumpTypToCnt(t, []string{"public.table_with_pk", "public.table_with_fk"}, []string{"public"})
	require.Equal(t, 1, itemTypToCnt["PRIMARY_KEY"])
	require.Equal(t, 1, itemTypToCnt["FK_CONSTRAINT"])
	require.Equal(t, 0, itemTypToCnt["POLICY"])

	// quoting names
	itemTypToCnt = extractPgDumpTypToCnt(t, []string{"ugly.ugly_table"}, []string{"ugly"})
	require.Equal(t, 1, itemTypToCnt["TYPE"])
	require.Equal(t, 1, itemTypToCnt["FUNCTION"])
	require.Equal(t, 1, itemTypToCnt["CAST"])

	// cast with function from other schema
	itemTypToCnt = extractPgDumpTypToCnt(t, []string{"ugly.ugly_table", "only_type.table"}, []string{"ugly", "only_type"})
	require.Equal(t, 2, itemTypToCnt["TYPE"])
	require.Equal(t, 2, itemTypToCnt["FUNCTION"])
	require.Equal(t, 2, itemTypToCnt["CAST"])

	// cast and function shouldn't be dumped
	itemTypToCnt = extractPgDumpTypToCnt(t, []string{"only_type.table"}, []string{"only_type"})
	require.Equal(t, 1, itemTypToCnt["TYPE"])
	require.Equal(t, 0, itemTypToCnt["FUNCTION"])
	require.Equal(t, 0, itemTypToCnt["CAST"])

	// with index attach
	itemTypToCnt = extractPgDumpTypToCnt(t, []string{"ia.ia_table", "ia.ia_part_1"}, []string{"ia"})
	require.Equal(t, 3, itemTypToCnt["INDEX"])
	require.Equal(t, 1, itemTypToCnt["INDEX_ATTACH"])
	require.Equal(t, 1, itemTypToCnt["TABLE_ATTACH"])

	// without index attach
	itemTypToCnt = extractPgDumpTypToCnt(t, []string{"ia.ia_table"}, []string{"ia"})
	require.Equal(t, 1, itemTypToCnt["INDEX"])
	require.Equal(t, 0, itemTypToCnt["INDEX_ATTACH"])
	require.Equal(t, 0, itemTypToCnt["TABLE_ATTACH"])

	// without index attach
	itemTypToCnt = extractPgDumpTypToCnt(t, []string{"ia.ia_part_1"}, []string{"ia"})
	require.Equal(t, 2, itemTypToCnt["INDEX"])
	require.Equal(t, 0, itemTypToCnt["INDEX_ATTACH"])
	require.Equal(t, 0, itemTypToCnt["TABLE_ATTACH"])

	// check function with regex with quote
	itemTypToCnt = extractPgDumpTypToCnt(t, []string{"only_functions.table_for_functions"}, []string{"only_functions"})
	require.Equal(t, 1, itemTypToCnt["FUNCTION"])

	// table attach with regex included dbtables like schema.*
	itemTypToCnt = extractPgDumpTypToCnt(t, []string{"public.*"}, []string{"public"})
	require.Equal(t, 2, itemTypToCnt["TABLE_ATTACH"])

	// Subtest to check that tablelist is intersection of endpoint and transfer, and endpoint exclude_tables.
	t.Run("table-list", func(t *testing.T) {
		t.Run("intersection", func(t *testing.T) {
			src := Source
			includedTable := "public.__test"
			includedTable2 := "public.table_with_pk"
			notIncludedTable := "public.table_with_fk"
			src.DBTables = []string{includedTable, includedTable2, notIncludedTable}
			transfer := helpers.MakeTransfer(helpers.TransferID, &src, &Target, abstract.TransferTypeSnapshotOnly)
			transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{includedTable, includedTable2}}
			items, err := postgres.ExtractPgDumpSchema(transfer)
			require.NoError(t, err)
			tableNames := set.New[string]()
			for _, i := range items {
				if i.Typ == "TABLE" {
					tableNames.Add(i.Schema + "." + i.Name)
				}
			}
			require.True(t, tableNames.Contains(includedTable))
			require.True(t, tableNames.Contains(includedTable2))
			require.False(t, tableNames.Contains(notIncludedTable))
		})

		// Subtest: endpoint exclude_tables is respected when include list is set
		t.Run("exclude_tables", func(t *testing.T) {
			src := Source
			excludedTable := "public.__test"
			notIncludedTable := "santa.Ho-Ho-Ho"
			src.DBTables = []string{"public.*"}
			src.ExcludedTables = []string{excludedTable}
			transfer := helpers.MakeTransfer(helpers.TransferID, &src, &Target, abstract.TransferTypeSnapshotOnly)
			items, err := postgres.ExtractPgDumpSchema(transfer)
			require.NoError(t, err)
			tableNames := set.New[string]()
			for _, i := range items {
				if i.Typ == "TABLE" {
					tableNames.Add(i.Schema + "." + i.Name)
				}
			}
			require.True(t, tableNames.Equals(set.New("public.table_with_fk", "public.table_with_pk", "public.wide_boys", "public.wide_boys_part_1", "public.wide_boys_part_2")))
			require.False(t, tableNames.Contains(excludedTable))
			require.False(t, tableNames.Contains(notIncludedTable))
		})
	})
}

func extractPgDumpTypToCnt(t *testing.T, DBTables []string, schemas []string) map[string]int {
	Source.DBTables = DBTables
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly)

	// clear target
	storage, err := postgres.NewStorage(targetAsSource.ToStorageParams(transfer))
	require.NoError(t, err)

	for _, schema := range schemas {
		_, err := storage.Conn.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE; CREATE SCHEMA %s;", schema, schema))
		require.NoError(t, err)
	}

	itemsSource, err := postgres.ExtractPgDumpSchema(transfer)
	require.NoError(t, err)

	// apply on target
	require.NoError(t, postgres.ApplyPgDumpPreSteps(itemsSource, transfer, &model.TransferOperation{}, helpers.EmptyRegistry()))
	require.NoError(t, postgres.ApplyPgDumpPostSteps(itemsSource, transfer, &model.TransferOperation{}, helpers.EmptyRegistry()))

	// make target a source and extract its schema
	targetAsSource.DBTables = Source.DBTables
	targetAsSource.PreSteps = Source.PreSteps
	targetAsSource.PostSteps = Source.PostSteps

	// compare schemas
	backwardFakeTransfer := helpers.MakeTransfer(helpers.TransferID, &targetAsSource, &Target, abstract.TransferTypeSnapshotOnly)
	itemsTarget, err := postgres.ExtractPgDumpSchema(backwardFakeTransfer)
	require.NoError(t, err)
	require.Equal(t, itemsSource, itemsTarget)

	itemTypToCnt := make(map[string]int)
	for _, i := range itemsSource {
		itemTypToCnt[i.Typ]++
	}

	return itemTypToCnt
}
