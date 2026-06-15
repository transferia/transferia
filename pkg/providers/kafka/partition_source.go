package kafka

import (
	"errors"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	queue_to_s3_parsequeue "github.com/transferia/transferia/pkg/parsequeue/queue_to_s3"
	kafka_reader "github.com/transferia/transferia/pkg/providers/kafka/reader"
	"github.com/transferia/transferia/pkg/util/queues/sequencer"
	"github.com/transferia/transferia/pkg/util/throttler"
	"go.ytsaurus.tech/library/go/core/log"
)

type PartitionSource struct {
	*Source

	partition int32
}

func (s *PartitionSource) Run(sink abstract.QueueToS3Sink) error {
	parseQ := queue_to_s3_parsequeue.New(s.logger, s.config.ParseQueueParallelism, sink, s.parseWithSynchronizeEvent, s.queueToS3Ack)

	var runErr error
	runErr = s.run(parseQ)
	parseQ.Close()

	if parseQ.Error() != nil {
		runErr = errors.Join(runErr, xerrors.Errorf("parse queue error: %w", parseQ.Error()))
	}

	return runErr
}

func (s *PartitionSource) queueToS3Ack(pushResult abstract.QueueResult) error {
	sequencerMessages := offsetsToQueueMessages(s.config.Topic, s.partition, pushResult.Offsets)
	commitMessages, err := s.sequencer.Pushed(sequencerMessages)
	if err != nil {
		return xerrors.Errorf("sequencer found an error in pushed messages, err: %w", err)
	}
	if err := s.reader.CommitMessages(s.ctx, recordsFromQueueMessages(commitMessages)...); err != nil {
		return xerrors.Errorf("failed to commit commit messages: %w", err)
	}

	s.logger.Info(
		"Commit messages done",
		log.String("pushed", sequencer.BuildMapPartitionToOffsetsRange(sequencerMessages)),
		log.String("committed", sequencer.BuildPartitionOffsetLogLine(commitMessages)),
	)

	return nil
}

func offsetsToQueueMessages(topic string, partition int32, offsets []uint64) []sequencer.QueueMessage {
	messages := make([]sequencer.QueueMessage, len(offsets))
	for i := range offsets {
		messages[i].Topic = topic
		messages[i].Partition = int(partition)
		messages[i].Offset = int64(offsets[i])
	}
	return messages
}

type PartitionDescription struct {
	Topic     string
	Partition int32
}

func NewPartitionSource(transferID string, cfg *KafkaSource, partitionDesc PartitionDescription, logger log.Logger, registry core_metrics.Registry) (*PartitionSource, error) {
	opts, err := kafkaClientCommonOptions(cfg)
	if err != nil {
		return nil, xerrors.Errorf("unable to build options for partition source: %w", err)
	}

	baseSource, err := newBaseSource(cfg, throttler.NewStubThrottler(), logger, registry)
	if err != nil {
		return nil, xerrors.Errorf("unable to create Source for partition: %w", err)
	}

	r, err := kafka_reader.NewPartitionReader(transferID, partitionDesc.Partition, partitionDesc.Topic, opts)
	if err != nil {
		return nil, xerrors.Errorf("unable to create reader for partition: %w", err)
	}
	baseSource.reader = r

	return &PartitionSource{
		Source:    baseSource,
		partition: partitionDesc.Partition,
	}, nil
}
