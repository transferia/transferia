package topicapisource

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	queue_to_s3_parsequeue "github.com/transferia/transferia/pkg/parsequeue/queue_to_s3"
	"github.com/transferia/transferia/pkg/parsers"
	topiccommon "github.com/transferia/transferia/pkg/providers/ydb/topics/common"
	topicsource "github.com/transferia/transferia/pkg/providers/ydb/topics/source"
	"github.com/transferia/transferia/pkg/providers/ydb/topics/source/topicapi/eventreader"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"go.ytsaurus.tech/library/go/core/log"
)

type PartitionSource struct {
	*Source
}

func (s *PartitionSource) Run(sink abstract.QueueToS3Sink) error {
	parseQ := queue_to_s3_parsequeue.NewWaitable(s.logger, s.config.ParseQueueParallelism, sink, s.parserWithCloudFunc(), s.queueToS3Ack)

	runErr := s.run(parseQ)
	parseQ.Close()

	if parseQ.Error() != nil {
		runErr = errors.Join(runErr, xerrors.Errorf("parse queue error: %w", parseQ.Error()))
	}

	return runErr
}

func (s *PartitionSource) ListPartitions() ([]abstract.Partition, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	topic := s.config.Topics[0]
	topicDescription, err := s.ydbClient.Topic().Describe(ctx, topic)
	if err != nil {
		return nil, xerrors.Errorf("describe topic error: %w", err)
	}
	if topicDescription.PartitionSettings.AutoPartitioningSettings.AutoPartitioningStrategy == topictypes.AutoPartitioningStrategyScaleUpAndDown {
		return nil, abstract.NewFatalError(xerrors.New("up and down auto partition scaling is not supported yet"))
	}

	var result []abstract.Partition
	for _, partition := range topicDescription.Partitions {
		result = append(result, abstract.Partition{
			Topic:     topic,
			Partition: uint32(partition.PartitionID),
		})
	}

	return result, nil
}

func (s *PartitionSource) queueToS3Ack(pushResult abstract.QueueResult) error {
	lastOffset := slices.Max(pushResult.Offsets)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	committableReader, ok := s.reader.(eventreader.OffsetCommitEventReader)
	if !ok {
		return xerrors.New("event reader does not support offset commit")
	}

	return committableReader.CommitOffset(ctx, lastOffset)
}

type PartitionDescription struct {
	Partition int64
}

func NewPartitionSource(cfg *topicsource.Config, partitionDesc PartitionDescription, parser parsers.Parser, logger log.Logger, metrics *stats.SourceStats) (*PartitionSource, error) {
	if len(cfg.Topics) > 1 || len(cfg.Topics) == 0 {
		return nil, abstract.NewFatalError(xerrors.New("only one topic reading is supported now"))
	}

	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()

	ydbClient, err := topiccommon.NewYDBDriver(cfg.Connection, logger)
	if err != nil {
		return nil, xerrors.Errorf("unable to create ydb client: %w", err)
	}
	rollbacks.Add(func() {
		_ = ydbClient.Close(context.Background())
	})

	reader, err := eventreader.NewPartitionEventReader(cfg.Consumer, cfg.Topics[0], partitionDesc.Partition, ydbClient, logger)
	if err != nil {
		return nil, xerrors.Errorf("unable to create event reader: %w", err)
	}
	rollbacks.Add(func() {
		_ = reader.Close(context.Background())
	})

	src, err := newBaseSource(cfg, parser, ydbClient, reader, logger, metrics)
	if err != nil {
		return nil, err
	}

	rollbacks.Cancel()

	return &PartitionSource{
		Source: src,
	}, nil
}
