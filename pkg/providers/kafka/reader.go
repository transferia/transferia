//go:build !disable_kafka_provider

package kafka

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/twmb/franz-go/pkg/kgo"
)

var errNoInput = xerrors.New("empty fetcher")

type franzReader struct {
	client *kgo.Client
}

func (r *franzReader) CommitMessages(ctx context.Context, msgs ...kgo.Record) error {
	forCommit := make([]*kgo.Record, len(msgs))
	for i := range msgs {
		msgs[i].LeaderEpoch = -1
		forCommit[i] = &msgs[i]
	}
	return r.client.CommitRecords(ctx, forCommit...)
}

// FetchMessage doesn't return pointer to struct, because franz-go has no guarantees about the returning values
func (r *franzReader) FetchMessage(ctx context.Context) (kgo.Record, error) {
	fetcher := r.client.PollRecords(ctx, 1)
	err := fetcher.Err()
	if err == nil && !fetcher.Empty() {
		return *fetcher.Records()[0], nil
	}
	if err == context.DeadlineExceeded || fetcher.Empty() {
		return kgo.Record{}, errNoInput
	}

	return kgo.Record{}, err
}

func (r *franzReader) Close() error {
	r.client.Close()
	return nil
}

func newFranzReader(cl *kgo.Client) reader {
	return &franzReader{
		client: cl,
	}
}
