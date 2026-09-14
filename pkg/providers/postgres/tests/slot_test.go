package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

func TestSlotHappyPath(t *testing.T) {
	src := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""))

	transferID := transferhelpers.GenerateTransferID("TestSlotHappyPath")
	src.SlotID = transferID

	connConfig, err := provider_postgres.MakeConnConfigFromSrc(logger.Log, src)
	require.NoError(t, err)
	conn, err := provider_postgres.NewPgConnPool(connConfig, logger.Log)
	require.NoError(t, err)

	slot, err := provider_postgres.NewSlot(conn, logger.Log, src)
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

	transferID := transferhelpers.GenerateTransferID("TestSlotBrokenConnection")
	src.SlotID = transferID

	connConfig, err := provider_postgres.MakeConnConfigFromSrc(logger.Log, src)
	require.NoError(t, err)
	conn, err := provider_postgres.NewPgConnPool(connConfig, logger.Log)
	require.NoError(t, err)

	slot, err := provider_postgres.NewSlot(conn, logger.Log, src)
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
