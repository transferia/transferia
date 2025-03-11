package permissions

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
)

func prepareSource() *postgres.PgSource {
	source := pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump"))
	source.User = "test_user"
	source.Password = "test_pass"
	return source
}

func TestTableListStar(t *testing.T) {
	source := prepareSource()

	storage, err := postgres.NewStorage(source.ToStorageParams(nil))
	require.NoError(t, err)

	tl, err := storage.TableList(nil)
	require.NoError(t, err)
	require.Len(t, tl, 1)
	require.Contains(t, tl, abstract.TableID{Namespace: "public", Name: "t_accessible"})
}

func TestTableListFilter(t *testing.T) {
	source := prepareSource()

	storage, err := postgres.NewStorage(source.ToStorageParams(nil))
	require.NoError(t, err)

	tl, err := storage.TableList(nil)
	require.NoError(t, err)
	require.Len(t, tl, 1)
	require.Contains(t, tl, abstract.TableID{Namespace: "public", Name: "t_accessible"})
}

// TestTableListFilterIncludeTables checks include directives do not affect `TableList` output
func TestTableListFilterIncludeTables(t *testing.T) {
	source := prepareSource()
	source.DBTables = []string{"\"public\".\"does_not_exist\""}

	storage, err := postgres.NewStorage(source.ToStorageParams(nil))
	require.NoError(t, err)

	_, err = storage.TableList(nil)
	require.NoError(t, err)
}
