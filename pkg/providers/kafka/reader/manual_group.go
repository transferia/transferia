package reader

import (
	"context"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/twmb/franz-go/pkg/kgo"
)

// fetchPartitionNextOffset retrieves the committed offset for a specific partition
// in a consumer group. Returns a -2 (AtStart) offset if no offset has been committed.
func fetchPartitionNextOffset(groupName string, partition int32, topic string, adminCl kafkaAdminClient) (kgo.Offset, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	offset := kgo.NewOffset().AtStart()
	if ok, err := groupExists(ctx, groupName, adminCl); err != nil {
		return kgo.Offset{}, xerrors.Errorf("failed to check group existence: %w", err)
	} else if !ok {
		return offset, nil
	}

	offsetResponses, err := adminCl.FetchOffsets(ctx, groupName)
	if err != nil {
		return kgo.Offset{}, xerrors.Errorf("failed to fetch offsets for topic %s partition %d: %w", topic, partition, err)
	}

	if topicPartitionOffsets, ok := offsetResponses[topic]; ok {
		if partitionOffset, ok := topicPartitionOffsets[partition]; ok {
			if partitionOffset.Err != nil {
				return kgo.Offset{}, xerrors.Errorf("topic %s partition %d offset response error: %w", topic, partition, partitionOffset.Err)
			}
			offset = offset.At(partitionOffset.At + 1)
		}
	}

	return offset, nil
}

func groupExists(ctx context.Context, groupName string, adminCl kafkaAdminClient) (bool, error) {
	listedGroups, err := adminCl.ListGroups(ctx)
	if err != nil {
		return false, xerrors.Errorf("failed to list groups: %w", err)
	}

	const DeadGroupState = "Dead"
	if group, ok := listedGroups[groupName]; !ok || group.State == DeadGroupState {
		return false, nil
	}

	return true, nil
}
