//go:build !disable_postgres_provider

package sequencer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProgressInfo(t *testing.T) {
	p := newProgressInfo()

	// check that we can not remove lsns from transaction that was not initially added
	require.Error(t, p.remove(1, nil))

	// check that we can add lsns to transaction and order is preserved
	require.NoError(t, p.add(1, 1))
	require.NoError(t, p.add(1, 2))
	require.NoError(t, p.add(1, 3))
	require.Error(t, p.add(1, 1))

	require.True(t, p.check(1))
	require.False(t, p.check(2))

	// check that we can only remove in increasing order
	require.Error(t, p.remove(1, []uint64{3, 2, 1}))
	require.NoError(t, p.remove(1, []uint64{1, 2, 3}))
	require.Equal(t, len(p.processing), len(p.transactionIDs))

	// check that we do not send last transaction in case it is not completed
	lastLsn := p.updateCommitted()
	require.Equal(t, uint64(0), lastLsn)

	require.NoError(t, p.add(2, 4))
	lastLsn = p.updateCommitted()
	require.Equal(t, uint64(3), lastLsn)
}
