package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract/model"
	error_codes "github.com/transferia/transferia/pkg/errors/codes"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/worker/tasks"
)

func TestPostgres(t *testing.T) {
	require.NoError(t, tasks.CheckEndpoint(context.Background(), &model.Transfer{Src: pgrecipe.RecipeSource(pgrecipe.WithPrefix(""))}))
	require.NoError(t, tasks.CheckEndpoint(context.Background(), &model.Transfer{Dst: pgrecipe.RecipeTarget(pgrecipe.WithPrefix(""))}))

	src := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""))
	src.User, src.Password = "no-such-user", "wrong-password"
	requireInvalidCredentials(t, &model.Transfer{Src: src})
}

func TestMySQL(t *testing.T) {
	require.NoError(t, tasks.CheckEndpoint(context.Background(), &model.Transfer{Src: mysqlrecipe.RecipeMysqlSource()}))
	require.NoError(t, tasks.CheckEndpoint(context.Background(), &model.Transfer{Dst: mysqlrecipe.RecipeMysqlTarget()}))

	src := mysqlrecipe.RecipeMysqlSource()
	src.User, src.Password = "no-such-user", "wrong-password"
	requireInvalidCredentials(t, &model.Transfer{Src: src})
}

func requireInvalidCredentials(t *testing.T, transfer *model.Transfer) {
	err := tasks.CheckEndpoint(context.Background(), transfer)
	require.Error(t, err)
	require.True(t, error_codes.InvalidCredential.Contains(err), err.Error())
}
