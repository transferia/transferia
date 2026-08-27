package postgres

import "github.com/transferia/transferia/pkg/abstract"

// PartitionLister exposes a single logical WAL partition for queue-to-s3 routing.
type PartitionLister struct {
	slotID string
}

func NewPartitionLister(slotID string) *PartitionLister {
	return &PartitionLister{slotID: slotID}
}

func (l *PartitionLister) ListPartitions() ([]abstract.Partition, error) {
	return []abstract.Partition{abstract.NewPartition(l.slotID, 0)}, nil
}

func (l *PartitionLister) Close() {}
