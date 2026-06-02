package reader

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type kafkaClient interface {
	PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches
	Close()
}

type kafkaAdminClient interface {
	ListGroups(ctx context.Context, filterStates ...string) (kadm.ListedGroups, error)
	FetchOffsets(ctx context.Context, group string) (kadm.OffsetResponses, error)
	CommitOffsets(ctx context.Context, group string, os kadm.Offsets) (kadm.OffsetResponses, error)
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
	fetches := r.client.PollRecords(ctx, 1)
	err := fetches.Err()
	if err == nil && !fetches.Empty() {
		return *fetches.Records()[0], nil
	}
	if xerrors.Is(err, context.DeadlineExceeded) || fetches.Empty() {
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

	adminClient := kadm.NewClient(client)
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
