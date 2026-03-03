package reader

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var ErrGroupNotFound = xerrors.New("group not found")

type kafkaClient interface {
	PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches
	Close()
}

type kafkaAdminClient interface {
	FetchOffsets(ctx context.Context, group string) (kadm.OffsetResponses, error)
	CommitOffsets(ctx context.Context, group string, os kadm.Offsets) (kadm.OffsetResponses, error)
	ListTopics(ctx context.Context, topics ...string) (kadm.TopicDetails, error)
}

type PartitionReader struct {
	group string

	client      kafkaClient
	adminClient kafkaAdminClient
}

func (r *PartitionReader) CommitMessages(ctx context.Context, msgs ...kgo.Record) error {
	if len(msgs) == 0 {
		return nil
	}

	responses, err := r.adminClient.CommitOffsets(ctx, r.group, offsetsFromMessages(msgs))
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

func (r *PartitionReader) ListPartitions(ctx context.Context, topic string) ([]int32, error) {
	topicDetailsResp, err := r.adminClient.ListTopics(ctx, topic)
	if err != nil {
		return nil, xerrors.Errorf("failed to get topic %s details: %w", topic, err)
	}

	topicDetails, ok := topicDetailsResp[topic]
	if !ok {
		return nil, xerrors.Errorf("list topics response does not contain topic %s", topic)
	}

	partitions := make([]int32, 0)
	for _, partitionDetails := range topicDetails.Partitions {
		if partitionDetails.Err != nil {
			return nil, xerrors.Errorf("list topic partitions response for %s contains error %w", topic, partitionDetails.Err)
		}
		partitions = append(partitions, partitionDetails.Partition)
	}

	return partitions, nil
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

	adminClient := kadm.NewClient(client)
	if err := groupExists(adminClient, group); err != nil {
		if xerrors.Is(err, ErrGroupNotFound) {
			if err := createConsumerGroup(group, clientOpts); err != nil {
				return nil, xerrors.Errorf("failed to create consumer group: %w", err)
			}
		} else {
			return nil, xerrors.Errorf("failed to check if consumer group exists: %w", err)
		}
	}

	offset, err := fetchPartitionNextOffset(group, partition, topic, adminClient)
	if err != nil {
		return nil, xerrors.Errorf("unable to get offsets: %w", err)
	}
	client.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		topic: {partition: offset},
	})

	return &PartitionReader{
		group:       group,
		adminClient: adminClient,
		client:      client,
	}, nil
}
