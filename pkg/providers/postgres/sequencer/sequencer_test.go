//go:build !disable_postgres_provider

package sequencer

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

func TestSequences(t *testing.T) {
	sequences := NewSequencer()
	require.NoError(t, sequences.StartProcessing([]abstract.ChangeItem{
		{ID: 1, LSN: 1}, {ID: 1, LSN: 2}, {ID: 1, LSN: 3},
	}))

	lastLsn, err := sequences.Pushed([]abstract.ChangeItem{
		{ID: 1, LSN: 1}, {ID: 1, LSN: 2},
	})
	require.NoError(t, err)
	require.Equal(t, lastLsn, uint64(0))

	require.NoError(t, sequences.StartProcessing([]abstract.ChangeItem{
		{ID: 2, LSN: 12},
	}))

	lastLsn, err = sequences.Pushed([]abstract.ChangeItem{
		{ID: 2, LSN: 12},
	})

	require.NoError(t, err)
	require.Equal(t, lastLsn, uint64(0))

	lastLsn, err = sequences.Pushed([]abstract.ChangeItem{
		{ID: 1, LSN: 3},
	})

	require.NoError(t, err)
	require.Equal(t, lastLsn, uint64(3))

	_, err = sequences.Pushed([]abstract.ChangeItem{
		{ID: 1, LSN: 3},
	})
	require.Error(t, err)

	_, err = sequences.Pushed([]abstract.ChangeItem{
		{ID: 3, LSN: 1},
	})
	require.Error(t, err)
}
