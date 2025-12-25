package reader

import (
	"context"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type kafkaClient interface {
	PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches
	Close()
}

type kafkaOffsetClient interface {
	FetchOffsets(ctx context.Context, group string) (kadm.OffsetResponses, error)
	CommitOffsets(ctx context.Context, group string, os kadm.Offsets) (kadm.OffsetResponses, error)
}

type PartitionReader struct {
	group string

	client       kafkaClient
	offsetClient kafkaOffsetClient
}

func (r *PartitionReader) CommitMessages(ctx context.Context, msgs ...kgo.Record) error {
	if len(msgs) == 0 {
		return nil
	}

	responses, err := r.offsetClient.CommitOffsets(ctx, r.group, offsetsFromMessages(msgs))
	if err != nil {
		return xerrors.Errorf("failed to commit offsets: %w", err)
	}
	return responses.Error()
}

// FetchMessage doesn't return pointer to struct, because franz-go has no guarantees about the returning values
func (r *PartitionReader) FetchMessage(ctx context.Context) (kgo.Record, error) {
	fetcher := r.client.PollRecords(ctx, 1)
	err := fetcher.Err()
	if err == nil && !fetcher.Empty() {
		return *fetcher.Records()[0], nil
	}
	if xerrors.Is(err, context.DeadlineExceeded) || fetcher.Empty() {
		return kgo.Record{}, ErrNoInput
	}

	return kgo.Record{}, err
}

func (r *PartitionReader) Close() error {
	r.client.Close()
	return nil
}

func offsetsFromMessages(msgs []kgo.Record) kadm.Offsets {
	topic := msgs[0].Topic
	partition := msgs[0].Partition
	offset := msgs[0].Offset

	for i := 1; i < len(msgs); i++ {
		offset = max(offset, msgs[i].Offset)
	}

	return map[string]map[int32]kadm.Offset{
		topic: {
			partition: kadm.Offset{
				Topic:       topic,
				Partition:   partition,
				At:          offset,
				LeaderEpoch: -1,
				Metadata:    "",
			},
		},
	}
}

func NewPartitionReader(group string, partition int32, topic string, clientOpts []kgo.Opt) (*PartitionReader, error) {
	client, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return nil, xerrors.Errorf("unable to create kafka client: %w", err)
	}

	if err := ensureTopicsExistWithRetries(client, topic); err != nil { // TODO move to a general code to avoid many requests per partition
		return nil, err
	}

	offsetClient := kadm.NewClient(client)
	offset, err := fetchPartitionNextOffset(group, partition, topic, offsetClient)
	if err != nil {
		// CoordinatorNotAvailable may be caused by the absence of the consumer group
		if xerrors.Is(err, kerr.CoordinatorNotAvailable) {
			if err := createConsumerGroup(group, clientOpts); err != nil {
				return nil, xerrors.Errorf("failed to create consumer group: %w", err)
			}
		} else {
			return nil, xerrors.Errorf("unable to get offsets: %w", err)
		}
	}
	client.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		topic: {partition: offset},
	})

	return &PartitionReader{
		group:        group,
		offsetClient: offsetClient,
		client:       client,
	}, nil
}

// fetchPartitionOffset retrieves the committed offset for a specific partition
// in a consumer group. Returns a zero offset if no offset has been committed.
func fetchPartitionNextOffset(group string, partition int32, topic string, offsetCl kafkaOffsetClient) (kgo.Offset, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	offsetResponses, err := offsetCl.FetchOffsets(ctx, group)
	if err != nil {
		return kgo.Offset{}, xerrors.Errorf("failed to fetch offsets for topic %s partition %d: %w", topic, partition, err)
	}

	offset := kgo.NewOffset()
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
