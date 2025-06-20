//go:build !disable_kafka_provider

package kafka

import (
	"context"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ abstract.Fetchable = (*sourceMultiTopics)(nil)

type sourceMultiTopics struct {
	src      *KafkaSource
	logger   log.Logger
	registry metrics.Registry
	topics   []string
}

func (t *sourceMultiTopics) Fetch() ([]abstract.ChangeItem, error) {
	brokers, err := ResolveBrokers(t.src.Connection)
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve brokers: %w", err)
	}

	if len(brokers) == 0 {
		return nil, xerrors.New("empty brokers")
	}
	var subSniffers []abstract.Fetchable
	samples := map[string]chan []abstract.ChangeItem{}

	for _, topic := range t.topics { // read sample for each topic, to ensure all results.
		srcCopy := *t.src
		srcCopy.Topic = topic
		srcCopy.GroupTopics = nil
		sniffer, err := NewSource(topic, &srcCopy, t.logger, t.registry)
		if err != nil {
			return nil, xerrors.Errorf("unable to create source: %w", err)
		}
		subSniffers = append(subSniffers, sniffer)
		samples[topic] = make(chan []abstract.ChangeItem, 1)
	}

	if err := util.ParallelDo(context.Background(), len(t.topics), 10, func(i int) error {
		sample, err := subSniffers[i].Fetch()
		if err != nil {
			if xerrors.Is(err, noDataErr) {
				samples[t.topics[i]] <- nil
				return nil
			}
			return xerrors.Errorf("unable to read sample from: %s: %w", t.topics[i], err)
		}
		samples[t.topics[i]] <- sample
		return nil
	}); err != nil {
		return nil, xerrors.Errorf("unable to read sample: %w", err)
	}
	var totalRes []abstract.ChangeItem
	for _, topic := range t.topics {
		totalRes = append(totalRes, <-samples[topic]...)
	}
	return totalRes, nil
}
