//go:build !disable_postgres_provider

package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
)

func TestSlotHappyPath(t *testing.T) {
	src := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""))

	connConfig, err := postgres.MakeConnConfigFromSrc(logger.Log, src)
	require.NoError(t, err)
	conn, err := postgres.NewPgConnPool(connConfig, logger.Log)
	require.NoError(t, err)

	slot, err := postgres.NewSlot(conn, logger.Log, src)
	require.NoError(t, err)
	require.NoError(t, slot.Create())

	exists, err := slot.Exist()
	require.NoError(t, err)
	require.True(t, exists)

	require.NoError(t, slot.Suicide())

	exists, err = slot.Exist()
	require.NoError(t, err)
	require.False(t, exists)
}

func TestSlotBrokenConnection(t *testing.T) {
	src := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""))

	connConfig, err := postgres.MakeConnConfigFromSrc(logger.Log, src)
	require.NoError(t, err)
	conn, err := postgres.NewPgConnPool(connConfig, logger.Log)
	require.NoError(t, err)

	slot, err := postgres.NewSlot(conn, logger.Log, src)
	require.NoError(t, err)
	require.NoError(t, slot.Create())

	exists, err := slot.Exist()
	require.NoError(t, err)
	require.True(t, exists)

	// emulate problems with db.
	conn.Close()

	_, err = slot.Exist()
	require.Error(t, err)
}
