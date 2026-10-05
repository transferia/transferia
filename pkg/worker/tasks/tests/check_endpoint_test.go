package tests

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	error_codes "github.com/transferia/transferia/pkg/errors/codes"
	"github.com/transferia/transferia/pkg/providers"
	"github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/worker/tasks"
)

// reporter records what CheckEndpoint reports; reportErr makes every report fail
type reporter struct {
	reportErr error
	connErr   error
	listed    bool
	tables    []abstract.TableID
	listErr   error
}

func (r *reporter) ReportConnectionCheck(_ string, checkErr error) error {
	r.connErr = checkErr
	return r.reportErr
}

func (r *reporter) ReportListTables(_ string, tables []abstract.TableID, listErr error) error {
	r.listed, r.tables, r.listErr = true, tables, listErr
	return r.reportErr
}

func checkEndpoint(transfer *model.Transfer, r *reporter) error {
	return tasks.CheckEndpoint(context.Background(), "operation-id", transfer, r, solomon.NewRegistry(nil))
}

func check(t *testing.T, transfer *model.Transfer) *reporter {
	r := new(reporter)
	require.NoError(t, checkEndpoint(transfer, r))
	return r
}

func TestPostgres(t *testing.T) {
	src := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""))
	pool, err := postgres.MakeConnPoolFromSrc(src, logger.Log)
	require.NoError(t, err)
	defer pool.Close()
	for _, table := range []string{"listed", "filtered_out"} {
		_, err = pool.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS "+table+" (id int primary key)")
		require.NoError(t, err)
	}

	r := check(t, &model.Transfer{Src: src})
	require.NoError(t, r.connErr)
	require.NoError(t, r.listErr)
	require.Contains(t, r.tables, abstract.TableID{Namespace: "public", Name: "listed"})
	require.Contains(t, r.tables, abstract.TableID{Namespace: "public", Name: "filtered_out"})
	require.True(t, slices.IsSortedFunc(r.tables, abstract.TableID.Less))

	src.DBTables = []string{"public.listed"}
	r = check(t, &model.Transfer{Src: src})
	require.Equal(t, []abstract.TableID{{Namespace: "public", Name: "listed"}}, r.tables)

	r = check(t, &model.Transfer{Src: src, DataObjects: &model.DataObjects{IncludeObjects: []string{"public.no_such"}}})
	require.NoError(t, r.listErr)
	require.Equal(t, []abstract.TableID{{Namespace: "public", Name: "listed"}}, r.tables, "the transfer filter does not apply")

	r = check(t, &model.Transfer{Dst: pgrecipe.RecipeTarget(pgrecipe.WithPrefix(""))})
	require.NoError(t, r.connErr)
	require.False(t, r.listed, "a target has no table listing")

	src.User, src.Password = "no-such-user", "wrong-password"
	r = check(t, &model.Transfer{Src: src})
	require.True(t, error_codes.InvalidCredential.Contains(r.connErr), r.connErr)
	require.False(t, r.listed, "no listing after a failed connection check")
}

func TestMySQL(t *testing.T) {
	src := mysqlrecipe.RecipeMysqlSource()
	connParams, err := mysql.NewConnectionParams(src.ToStorageParams())
	require.NoError(t, err)
	db, err := mysql.Connect(connParams, nil)
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS listed (id int primary key)")
	require.NoError(t, err)

	r := check(t, &model.Transfer{Src: src})
	require.NoError(t, r.connErr)
	require.NoError(t, r.listErr)
	require.Contains(t, r.tables, abstract.TableID{Namespace: src.Database, Name: "listed"})

	r = check(t, &model.Transfer{Dst: mysqlrecipe.RecipeMysqlTarget()})
	require.NoError(t, r.connErr)
	require.False(t, r.listed, "a target has no table listing")

	src.User, src.Password = "no-such-user", "wrong-password"
	r = check(t, &model.Transfer{Src: src})
	require.True(t, error_codes.InvalidCredential.Contains(r.connErr), r.connErr)
	require.False(t, r.listed, "no listing after a failed connection check")
}

func TestListTablesContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, src := range []model.Source{pgrecipe.RecipeSource(pgrecipe.WithPrefix("")), mysqlrecipe.RecipeMysqlSource()} {
		lister, ok := providers.Source[providers.TableLister](
			logger.Log, solomon.NewRegistry(nil), coordinator.NewFakeClient(), &model.Transfer{Src: src},
		)
		require.True(t, ok)
		_, err := lister.ListTables(ctx)
		require.ErrorIs(t, err, context.Canceled, src.GetProviderType())
	}
}

func TestInternalErrors(t *testing.T) {
	src := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""))
	require.Error(t, checkEndpoint(&model.Transfer{Src: src}, &reporter{reportErr: xerrors.New("cp is down")}))
	require.Error(t, checkEndpoint(&model.Transfer{}, new(reporter)))
}

func TestNotSupported(t *testing.T) {
	r := check(t, &model.Transfer{Src: new(model.MockSource)})
	require.True(t, error_codes.CheckEndpointNotSupported.Contains(r.connErr), r.connErr)
	require.False(t, r.listed)
}
