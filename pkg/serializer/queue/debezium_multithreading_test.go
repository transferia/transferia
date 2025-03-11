package queue

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

func TestMergeWithMaxMessageSize(t *testing.T) {
	tableID := abstract.TableID{
		Namespace: "namespace",
		Name:      "name",
	}
	tablePartID := abstract.TablePartID{
		TableID: tableID,
		PartID:  "",
	}

	in0 := make(map[abstract.TablePartID][]SerializedMessage)
	in0[tablePartID] = []SerializedMessage{{Key: []byte{}, Value: []byte{0, 0}}}
	task0 := task{
		serializer:     nil,
		sessionPackers: nil,
		changeItems:    nil,
		results:        in0,
		err:            nil,
	}

	in1 := make(map[abstract.TablePartID][]SerializedMessage)
	in1[tablePartID] = []SerializedMessage{{Key: []byte{}, Value: []byte{1, 1}}}
	task1 := task{
		serializer:     nil,
		sessionPackers: nil,
		changeItems:    nil,
		results:        in1,
		err:            nil,
	}

	// test with big maxMessageSize

	result0 := MergeWithMaxMessageSize([]*task{&task0, &task1}, 999999)
	require.Equal(t, 1, len(result0))
	for _, v := range result0 {
		require.Equal(t, 1, len(v))
		for _, vv := range v {
			require.Equal(t, []byte{0, 0, 1, 1}, vv.Value)
		}
	}

	// test with maxMessageSize==0

	result1 := MergeWithMaxMessageSize([]*task{&task0, &task1}, 1)
	require.Equal(t, 1, len(result1))
	for _, v := range result1 {
		require.Equal(t, 2, len(v))
		for i, vv := range v {
			switch i {
			case 0:
				require.Equal(t, []byte{0, 0}, vv.Value)
			case 1:
				require.Equal(t, []byte{1, 1}, vv.Value)
			}
		}
	}
}
