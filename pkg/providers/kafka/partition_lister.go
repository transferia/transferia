package kafka

import (
	"context"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type PartitionLister struct {
	topics      []string
	adminClient *kadm.Client
}

func (l *PartitionLister) ListPartitions() ([]abstract.Partition, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	topicDetailsResp, err := l.adminClient.ListTopics(ctx, l.topics...)
	if err != nil {
		return nil, xerrors.Errorf("unable to list partitions: %w", err)
	}

	partitions := make([]abstract.Partition, 0)
	for _, topic := range l.topics {
		topicDetails, ok := topicDetailsResp[topic]
		if !ok {
			return nil, xerrors.Errorf("list topics response does not contain topic %s", topic)
		}

		for _, partitionDetails := range topicDetails.Partitions {
			if partitionDetails.Err != nil {
				return nil, xerrors.Errorf("list topic partitions response for %s contains error: %w", topic, partitionDetails.Err)
			}

			partitions = append(partitions, abstract.Partition{
				Topic:     topic,
				Partition: uint32(partitionDetails.Partition),
			})
		}
	}

	return partitions, nil
}

func (l *PartitionLister) Close() {
	l.adminClient.Close()
}

func NewPartitionLister(cfg *KafkaSource) (*PartitionLister, error) {
	if err := cfg.WithConnectionID(); err != nil {
		return nil, xerrors.Errorf("unable to resolve connection for partition lister: %w", err)
	}

	opts, err := kafkaClientCommonOptions(cfg)
	if err != nil {
		return nil, xerrors.Errorf("unable to build options for partition lister: %w", err)
	}

	if err := checkTopicsExistence(opts, cfg.Topics()); err != nil {
		return nil, xerrors.Errorf("unable to check topic existence for partition lister: %w", err)
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, xerrors.Errorf("unable to create kafka client for partition lister: %w", err)
	}

	return &PartitionLister{
		topics:      cfg.Topics(),
		adminClient: kadm.NewClient(client),
	}, nil
}
