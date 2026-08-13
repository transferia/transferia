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

func TestPushedOffsets(t *testing.T) {
	t.Run("tx ordering and partial tx does not advance committed", func(t *testing.T) {
		sequences := NewSequencer()
		require.NoError(t, sequences.StartProcessing([]abstract.ChangeItem{
			{ID: 1, LSN: 1}, {ID: 1, LSN: 2}, {ID: 1, LSN: 3},
		}))

		lastLsn, err := sequences.PushedOffsets([]uint64{1, 2})
		require.NoError(t, err)
		require.Equal(t, uint64(0), lastLsn)

		require.NoError(t, sequences.StartProcessing([]abstract.ChangeItem{
			{ID: 2, LSN: 12},
		}))

		lastLsn, err = sequences.PushedOffsets([]uint64{12})
		require.NoError(t, err)
		require.Equal(t, uint64(0), lastLsn)

		lastLsn, err = sequences.PushedOffsets([]uint64{3})
		require.NoError(t, err)
		require.Equal(t, uint64(3), lastLsn)
	})

	t.Run("duplicate lsns within one tx", func(t *testing.T) {
		sequences := NewSequencer()
		require.NoError(t, sequences.StartProcessing([]abstract.ChangeItem{
			{ID: 1, LSN: 10}, {ID: 1, LSN: 10}, {ID: 1, LSN: 11},
		}))
		require.NoError(t, sequences.StartProcessing([]abstract.ChangeItem{
			{ID: 2, LSN: 20},
		}))

		lastLsn, err := sequences.PushedOffsets([]uint64{10, 10, 11})
		require.NoError(t, err)
		require.Equal(t, uint64(11), lastLsn)
	})

	t.Run("unknown lsn is error", func(t *testing.T) {
		sequences := NewSequencer()
		require.NoError(t, sequences.StartProcessing([]abstract.ChangeItem{
			{ID: 1, LSN: 1},
		}))
		_, err := sequences.PushedOffsets([]uint64{2})
		require.Error(t, err)
	})

	t.Run("decreasing offsets is error", func(t *testing.T) {
		sequences := NewSequencer()
		require.NoError(t, sequences.StartProcessing([]abstract.ChangeItem{
			{ID: 1, LSN: 1}, {ID: 1, LSN: 2},
		}))
		_, err := sequences.PushedOffsets([]uint64{2, 1})
		require.Error(t, err)
	})
}
