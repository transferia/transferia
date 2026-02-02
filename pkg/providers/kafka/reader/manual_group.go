package reader

import (
	"context"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// fetchPartitionNextOffset retrieves the committed offset for a specific partition
// in a consumer group. Returns a -2 (AtStart) offset if no offset has been committed.
func fetchPartitionNextOffset(group string, partition int32, topic string, offsetCl kafkaOffsetClient) (kgo.Offset, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	offsetResponses, err := offsetCl.FetchOffsets(ctx, group)
	if err != nil {
		return kgo.Offset{}, xerrors.Errorf("failed to fetch offsets for topic %s partition %d: %w", topic, partition, err)
	}

	offset := kgo.NewOffset().AtStart()
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

// groupExists gets the group description and returns ErrGroupNotFound if the group wasn't found or the group is dead
func groupExists(admCl *kadm.Client, group string) error {
	groupDescriptions, err := admCl.DescribeGroups(context.TODO(), group)
	if err != nil {
		// CoordinatorNotAvailable may be caused by the absence of the consumer group
		var shardErrs *kadm.ShardErrors
		if xerrors.As(err, &shardErrs) {
			for _, shardErr := range shardErrs.Errs {
				if xerrors.Is(shardErr.Err, kerr.CoordinatorNotAvailable) { // PROBLEM IS HERE
					return ErrGroupNotFound
				}
			}
		}

		return xerrors.Errorf("unable to describe group %s: %w", group, err)
	}
	if groupDescriptions.Error() != nil {
		return xerrors.Errorf("group descriptions response error %s: %w", group, err)
	}

	groupDescription, err := groupDescriptions.On(group, nil)
	if err != nil {
		if xerrors.Is(err, kerr.GroupIDNotFound) {
			return ErrGroupNotFound
		}
		return xerrors.Errorf("problem with description for group %s: %w", group, err)
	}

	const DeadGroupState = "Dead"
	if groupDescription.State == DeadGroupState {
		return ErrGroupNotFound
	}

	return nil
}

// createConsumerGroup creates a consumer group in Kafka by initializing a client
// and performing a single poll operation. The group is created lazily on first poll.
func createConsumerGroup(group string, clientOpts []kgo.Opt) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientOptsWithGroup := append(clientOpts,
		kgo.ConsumerGroup(group),
		kgo.DisableAutoCommit(),
	)
	client, err := kgo.NewClient(clientOptsWithGroup...)
	if err != nil {
		return xerrors.Errorf("unable to create kafka client to initialize consumer group: %w", err)
	}
	defer client.Close()

	_ = client.PollRecords(ctx, 1)

	return nil
}
