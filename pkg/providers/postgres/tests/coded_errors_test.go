package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/errors/coded"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/sink"
)

func TestCodedErrors_DropTableWithDependencies(t *testing.T) {
	target := pgrecipe.RecipeTarget(pgrecipe.WithPrefix(""))
	time.Sleep(10 * time.Second)
	storage, err := postgres.NewStorage(target.ToStorageParams())
	require.NoError(t, err)

	ctx := context.Background()

	// Создаем таблицу с зависимостями
	_, err = storage.Conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS parent_table (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255)
		);
	`)
	require.NoError(t, err)

	// Создаем дочернюю таблицу с внешним ключом
	_, err = storage.Conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS child_table (
			id SERIAL PRIMARY KEY,
			parent_id INTEGER REFERENCES parent_table(id),
			description TEXT
		);
	`)
	require.NoError(t, err)
	//
	//// Создаем представление, которое зависит от таблицы
	//_, err = storage.Conn.Exec(ctx, `
	//	CREATE OR REPLACE VIEW parent_view AS
	//	SELECT * FROM parent_table;
	//`)
	//require.NoError(t, err)

	// Отправляем ChangeItem для выполнения cleanup
	changeItem := abstract.ChangeItem{
		Kind:   abstract.DropTableKind,
		Table:  "parent_table",
		Schema: "public",
	}
	transfer := &model.Transfer{
		Dst: target,
	}
	sink, err := sink.ConstructBaseSink(
		transfer,
		logger.Log,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
		coordinator.NewFakeClient(),
		middlewares.Config{},
	)
	require.NoError(t, err)
	err = sink.Push([]abstract.ChangeItem{changeItem})
	require.Error(t, err)
	var codedErr coded.CodedError
	require.ErrorAs(t, err, &codedErr)
	require.Equal(t, postgres.DropTableWithDependenciesCode, codedErr.Code())
}
