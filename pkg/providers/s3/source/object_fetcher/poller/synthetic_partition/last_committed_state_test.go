package synthetic_partition

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/providers/s3/source/object_fetcher/poller/dispatcher/file"
)

func TestLastCommittedState(t *testing.T) {
	nsecToTime := func(nsec int64) time.Time {
		return time.Unix(0, nsec)
	}

	state, err := newLastCommittedState(1, []*file.File{{FileName: "a", LastModified: nsecToTime(1)}})
	require.NoError(t, err)
	stateStr := state.ToString()
	fmt.Println(stateStr)

	newState, err := newLastCommittedState(0, nil)
	require.NoError(t, err)
	newState.FromString(stateStr)
	require.Equal(t, newState.ns, int64(1))
	require.True(t, newState.files.Contains("a"))
}
