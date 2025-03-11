// This test checks `pg.Storage.TableList()` works properly for different kinds of objects

package pg

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	"github.com/transferria/transferria/tests/helpers"
)

var dumpRs = map[string]idAndColumnNames{
	"public.common":                {ID: abstract.TableID{Namespace: "public", Name: "common"}, CNs: []string{"i", "t"}},
	"public.empty":                 {ID: abstract.TableID{Namespace: "public", Name: "empty"}, CNs: []string{"i", "t"}},
	"public.v":                     {ID: abstract.TableID{Namespace: "public", Name: "v"}, CNs: []string{"i", "t"}},
	"public.mv":                    {ID: abstract.TableID{Namespace: "public", Name: "mv"}, CNs: []string{"i", "t"}},
	"extra.common":                 {ID: abstract.TableID{Namespace: "extra", Name: "common"}, CNs: []string{"i", "t"}},
	"extra.empty":                  {ID: abstract.TableID{Namespace: "extra", Name: "empty"}, CNs: []string{"i", "t"}},
	"extrablocked.common":          {ID: abstract.TableID{Namespace: "extrablocked", Name: "common"}, CNs: []string{"t", "i"}},
	"extrablocked.emptywithselect": {ID: abstract.TableID{Namespace: "extrablocked", Name: "emptywithselect"}, CNs: []string{"i", "t"}},
	"columnaccess.table":           {ID: abstract.TableID{Namespace: "columnaccess", Name: "table"}, CNs: []string{"i", "r", "ur"}},
}

type idAndColumnNames struct {
	ID  abstract.TableID
	CNs []string
}

var SourceAllPrivileges = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("dump"))

var SourceRestrictedPrivileges = *pgrecipe.RecipeSource(
	pgrecipe.WithPrefix(""),
	pgrecipe.WithInitDir("dump"),
	pgrecipe.WithEdit(func(pg *postgres.PgSource) {
		pg.User = "blockeduser"
		pg.Password = "sim-sim@OPEN"
	}),
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	SourceAllPrivileges.WithDefaults()
	SourceRestrictedPrivileges.WithDefaults()
}

func checkTableInMapWithColumns(t *testing.T, expected idAndColumnNames, actualMap abstract.TableMap) {
	require.Contains(t, actualMap, expected.ID)
	checkColumnNames(t, expected.CNs, actualMap[expected.ID])
}

func checkColumnNames(t *testing.T, expected []string, actual abstract.TableInfo) {
	require.Equal(t, len(expected), len(actual.Schema.Columns()))
	for i, c := range actual.Schema.Columns() {
		require.Equal(t, expected[i], c.ColumnName)
	}
}

func checkTableNotInMap(t *testing.T, expected abstract.TableID, actualMap abstract.TableMap) {
	require.NotContains(t, actualMap, expected)
}

func TestTableListStarAllPrivileges(t *testing.T) {
	src := &SourceAllPrivileges

	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: src.Port},
	))

	storage, err := postgres.NewStorage(src.ToStorageParams(nil))
	require.NoError(t, err)

	extract, err := storage.TableList(nil)
	require.NoError(t, err)

	checkTableInMapWithColumns(t, dumpRs["public.common"], extract)
	checkTableInMapWithColumns(t, dumpRs["public.empty"], extract)
	checkTableInMapWithColumns(t, dumpRs["public.v"], extract)
	checkTableNotInMap(t, dumpRs["public.mv"].ID, extract)
	checkTableInMapWithColumns(t, dumpRs["extra.common"], extract)
	checkTableInMapWithColumns(t, dumpRs["extra.empty"], extract)
	checkTableInMapWithColumns(t, dumpRs["extrablocked.common"], extract)
	checkTableInMapWithColumns(t, dumpRs["extrablocked.emptywithselect"], extract)
	checkTableInMapWithColumns(t, dumpRs["columnaccess.table"], extract)

	require.Equal(t, 8, len(extract))
}

func TestTableListStarRestrictedPrivileges(t *testing.T) {
	src := &SourceRestrictedPrivileges

	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: src.Port},
	))

	storage, err := postgres.NewStorage(src.ToStorageParams(nil))
	require.NoError(t, err)

	extract, err := storage.TableList(nil)
	require.NoError(t, err)

	checkTableInMapWithColumns(t, dumpRs["public.common"], extract)
	checkTableInMapWithColumns(t, dumpRs["public.empty"], extract)
	checkTableInMapWithColumns(t, dumpRs["public.v"], extract)
	checkTableNotInMap(t, dumpRs["public.mv"].ID, extract)
	checkTableInMapWithColumns(t, dumpRs["extra.common"], extract)
	checkTableInMapWithColumns(t, dumpRs["extra.empty"], extract)
	checkTableNotInMap(t, dumpRs["extrablocked.common"].ID, extract)
	checkTableNotInMap(t, dumpRs["extrablocked.emptywithselect"].ID, extract)
	checkTableNotInMap(t, dumpRs["columnaccess.table"].ID, extract)

	require.Equal(t, 5, len(extract))
}

func TestTableListPublicAllPrivileges(t *testing.T) {
	src := &SourceAllPrivileges

	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: src.Port},
	))

	storage, err := postgres.NewStorage(src.ToStorageParams(nil))
	require.NoError(t, err)

	extract, err := storage.TableList(nil)
	require.NoError(t, err)

	// remove not 'public' schema entries
	for {
		found := false
		for k := range extract {
			if k.Namespace != "public" {
				found = true
				delete(extract, k)
			}
		}
		if !found {
			break
		}
	}

	checkTableInMapWithColumns(t, dumpRs["public.common"], extract)
	checkTableInMapWithColumns(t, dumpRs["public.empty"], extract)
	checkTableInMapWithColumns(t, dumpRs["public.v"], extract)
	checkTableNotInMap(t, dumpRs["public.mv"].ID, extract)

	require.Equal(t, 3, len(extract))
}
