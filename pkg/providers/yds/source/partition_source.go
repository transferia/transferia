package source

import (
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	topicapisource "github.com/transferia/transferia/pkg/providers/ydb/topics/source/topicapi"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewPartitionLister(transferID string, cfg *YDSSource, logger log.Logger) (abstract.PartitionLister, error) {
	if err := resolveCredentialsIfNeeded(cfg, logger); err != nil {
		return nil, xerrors.Errorf("unable to resolve credentials for partition lister: %w", err)
	}

	topicSourceCfg := buildTopicSourceConfig(transferID, cfg)

	return topicapisource.NewPartitionLister(topicSourceCfg, logger)
}

func NewPartitionSource(transferID string, cfg *YDSSource, partition abstract.Partition, logger log.Logger, registry metrics.Registry) (abstract.QueueToS3Source, error) {
	if err := resolveCredentialsIfNeeded(cfg, logger); err != nil {
		return nil, xerrors.Errorf("unable to resolve credentials for partition source: %w", err)
	}

	sourceMetrics := stats.NewSourceStats(registry)
	parser, err := createParser(cfg, logger, sourceMetrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to create parser for partition source: %w", err)
	}

	topicSourceCfg := buildTopicSourceConfig(transferID, cfg)

	return topicapisource.NewPartitionSource(
		topicSourceCfg,
		topicapisource.PartitionDescription{
			Topic:     partition.Topic,
			Partition: int64(partition.Partition),
		},
		parser,
		logger,
		sourceMetrics,
	)
}
