package topicapisource

import (
	"context"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	topiccommon "github.com/transferia/transferia/pkg/providers/ydb/topics/common"
	topicsource "github.com/transferia/transferia/pkg/providers/ydb/topics/source"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"go.ytsaurus.tech/library/go/core/log"
)

type PartitionLister struct {
	topics    []string
	ydbClient *ydb.Driver

	logger log.Logger
}

func (l *PartitionLister) ListPartitions() ([]abstract.Partition, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var result []abstract.Partition
	for _, topic := range l.topics {
		topicDescription, err := l.ydbClient.Topic().Describe(ctx, topic)
		if err != nil {
			return nil, xerrors.Errorf("describe topic error: %w", err)
		}
		if topicDescription.PartitionSettings.AutoPartitioningSettings.AutoPartitioningStrategy == topictypes.AutoPartitioningStrategyScaleUpAndDown {
			return nil, abstract.NewFatalError(xerrors.New("up and down auto partition scaling is not supported yet"))
		}

		for _, partition := range topicDescription.Partitions {
			result = append(result, abstract.Partition{
				Topic:     topic,
				Partition: uint32(partition.PartitionID),
			})
		}
	}

	return result, nil
}

func (l *PartitionLister) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := l.ydbClient.Close(ctx); err != nil {
		l.logger.Warn("unable to close ydb partition lister", log.Error(err))
	}
}

func NewPartitionLister(cfg *topicsource.Config, logger log.Logger) (*PartitionLister, error) {
	ydbClient, err := topiccommon.NewYDBDriver(cfg.Connection, logger)
	if err != nil {
		return nil, xerrors.Errorf("unable to create ydb client for partition lister: %w", err)
	}

	return &PartitionLister{
		topics:    cfg.Topics,
		ydbClient: ydbClient,
		logger:    logger,
	}, nil
}
