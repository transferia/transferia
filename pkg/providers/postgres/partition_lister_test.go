package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

func TestPartitionListerListPartitions(t *testing.T) {
	lister := NewPartitionLister("slot-abc")
	partitions, err := lister.ListPartitions()
	require.NoError(t, err)
	require.Equal(t, []abstract.Partition{{
		Topic:     "slot-abc",
		Partition: 0,
	}}, partitions)
	lister.Close()
}
