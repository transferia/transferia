package permissions

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/tests/helpers/mysql"
)

func prepareSource() *provider_mysql.MysqlSource {
	source := mysql.RecipeMysqlSource()
	source.User = "test_user"
	source.Password = "test_pass"
	return source
}

func TestTableListError(t *testing.T) {
	source := prepareSource()

	storage, err := provider_mysql.NewStorage(source.ToStorageParams())
	require.NoError(t, err)

	_, err = storage.TableList(nil)
	require.Error(t, err)
}

func TestTableListNoError(t *testing.T) {
	source := mysql.WithMysqlInclude(prepareSource(), []string{"foo"})

	storage, err := provider_mysql.NewStorage(source.ToStorageParams())
	require.NoError(t, err)

	tables, err := storage.TableList(nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(tables))
	_, containsFoo := tables[abstract.TableID{Namespace: "source", Name: "foo"}]
	require.True(t, containsFoo)
}
