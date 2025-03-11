package permissions

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/providers/mysql"
	"github.com/transferria/transferria/tests/helpers"
)

func prepareSource() *mysql.MysqlSource {
	source := helpers.RecipeMysqlSource()
	source.User = "test_user"
	source.Password = "test_pass"
	return source
}

func TestTableListError(t *testing.T) {
	source := prepareSource()

	storage, err := mysql.NewStorage(source.ToStorageParams())
	require.NoError(t, err)

	_, err = storage.TableList(nil)
	require.Error(t, err)
}

func TestTableListNoError(t *testing.T) {
	source := helpers.WithMysqlInclude(prepareSource(), []string{"foo"})

	storage, err := mysql.NewStorage(source.ToStorageParams())
	require.NoError(t, err)

	tables, err := storage.TableList(nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(tables))
	_, containsFoo := tables[abstract.TableID{Namespace: "source", Name: "foo"}]
	require.True(t, containsFoo)
}
