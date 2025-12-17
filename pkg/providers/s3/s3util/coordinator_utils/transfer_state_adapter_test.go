package coordinator_utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/s3/sink/testutil"
)

func TestThrottling(t *testing.T) {
	transferID := "dtt"
	dpClient := testutil.NewFakeClientWithTransferState()
	coordinatorStateAdapter := NewTransferStateAdapter(dpClient, 10, transferID)

	var err error

	nsecToTime := func(nsec int64) time.Time {
		return time.Unix(0, nsec)
	}

	coordinatorStateAdapter.NowGetter = func() time.Time { return nsecToTime(1) }
	err = coordinatorStateAdapter.SetTransferState(
		map[string]*coordinator.TransferStateData{
			transferID: {
				Generic: "OLD",
			},
		},
	)
	require.NoError(t, err)
	require.Equal(t, `OLD`, dpClient.GetTransferStateForTests(t)[transferID].Generic)

	coordinatorStateAdapter.NowGetter = func() time.Time { return nsecToTime(2) }
	err = coordinatorStateAdapter.SetTransferState(
		map[string]*coordinator.TransferStateData{
			transferID: {
				Generic: "NEW",
			},
		},
	)
	require.NoError(t, err)
	require.Equal(t, `OLD`, dpClient.GetTransferStateForTests(t)[transferID].Generic)

	coordinatorStateAdapter.NowGetter = func() time.Time { return nsecToTime(13) }
	err = coordinatorStateAdapter.SetTransferState(
		map[string]*coordinator.TransferStateData{
			transferID: {
				Generic: "NEW2",
			},
		},
	)
	require.NoError(t, err)
	require.Equal(t, `NEW2`, dpClient.GetTransferStateForTests(t)[transferID].Generic)
}
