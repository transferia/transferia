package kafka

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/errors/coded"
	error_codes "github.com/transferia/transferia/pkg/errors/codes"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func checkTopicsExistence(clientOpts []kgo.Opt, topics []string) error {
	kfClient, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return xerrors.Errorf("unable to create kafka client to ensure topics: %w", err)
	}
	defer kfClient.Close()

	if err := backoff.Retry(func() error {
		return ensureTopicExists(kfClient, 15*time.Second, topics)
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 5)); err != nil {
		return xerrors.Errorf("unable to ensure topic existence: %w", err)
	}

	return nil
}

func ensureTopicExists(requestor kmsg.Requestor, timeout time.Duration, topics []string) error {
	req := kmsg.NewMetadataRequest()
	for _, topic := range topics {
		reqTopic := kmsg.NewMetadataRequestTopic()
		reqTopic.Topic = kmsg.StringPtr(topic)
		req.Topics = append(req.Topics, reqTopic)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	resp, err := req.RequestWith(ctx, requestor)
	if err != nil {
		return xerrors.Errorf("unable to check topics existence: %w", err)
	}
	missedTopics := make([]string, 0)
	for _, t := range resp.Topics {
		if t.ErrorCode != kerr.UnknownTopicOrPartition.Code {
			continue
		}
		// despite topic error we still got some partitions
		if len(t.Partitions) > 0 {
			continue
		}

		name := ""
		if t.Topic != nil {
			name = *t.Topic
		}
		missedTopics = append(missedTopics, name)
	}
	if len(missedTopics) != 0 {
		return abstract.NewFatalError(coded.Errorf(error_codes.MissingData, "%v not found", missedTopics))
	}

	return nil
}
