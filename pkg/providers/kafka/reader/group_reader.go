package reader

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/twmb/franz-go/pkg/kgo"
)

type GroupReader struct {
	client *kgo.Client
}

func (r *GroupReader) CommitMessages(ctx context.Context, msgs ...kgo.Record) error {
	forCommit := make([]*kgo.Record, len(msgs))
	for i := range msgs {
		msgs[i].LeaderEpoch = -1
		forCommit[i] = &msgs[i]
	}
	return r.client.CommitRecords(ctx, forCommit...)
}

// FetchMessage doesn't return pointer to struct, because franz-go has no guarantees about the returning values
func (r *GroupReader) FetchMessage(ctx context.Context) (kgo.Record, error) {
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

func (r *GroupReader) Close() error {
	r.client.Close()
	return nil
}

func NewGroupReader(group string, topics []string, clientOpts []kgo.Opt) (*GroupReader, error) {
	clientOpts = append(clientOpts,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
	)

	client, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return nil, xerrors.Errorf("unable to create kafka client: %w", err)
	}

	if err := ensureTopicsExistWithRetries(client, topics...); err != nil {
		return nil, err
	}

	return &GroupReader{
		client: client,
	}, nil
}
