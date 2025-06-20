//go:build !disable_postgres_provider

package sequencer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLsnTransaction(t *testing.T) {
	p := newLsnTransaction()
	// check that add only in incresing order
	require.NoError(t, p.appendLsn(1))
	require.NoError(t, p.appendLsn(2))
	require.NoError(t, p.appendLsn(3))
	require.Error(t, p.appendLsn(1))

	// check that stract parameters update correctly
	require.Equal(t, uint64(3), p.lastLsn)
	require.Equal(t, 3, len(p.lsns))

	// check that we can not remove lsns in nonincreasing order
	require.Error(t, p.removeLsn([]uint64{3, 2, 1}))

	// check that we successfully remove lsns and update parameters
	require.NoError(t, p.removeLsn([]uint64{1, 2, 3}))
	require.Equal(t, 0, len(p.lsns))
	require.Equal(t, uint64(3), p.lastLsn)

	// check that we cannot remove lsns whih are not present
	require.Error(t, p.removeLsn([]uint64{1, 2, 3}))

	// check that order is preserved even when no lsns are currently processing
	require.Error(t, p.appendLsn(1))
}
