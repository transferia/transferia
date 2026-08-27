package logbroker

import (
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	parsers_resources "github.com/transferia/transferia/pkg/parsers/resources"
	topicapisource "github.com/transferia/transferia/pkg/providers/ydb/topics/source/topicapi"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewPartitionSource(cfg *LfSource, partition abstract.Partition, logger log.Logger, registry core_metrics.Registry) (abstract.QueueToS3Source, error) {
	if partition.Cluster == "" {
		return nil, xerrors.Errorf("partition cluster must not be empty: %s", partition.String())
	}

	sourceMetrics := stats.NewSourceStats(registry)

	var err error
	var parser parsers.Parser
	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()
	if cfg.ParserConfig != nil {
		parser, err = parsers.NewParserFromMap(cfg.ParserConfig, false, logger, sourceMetrics)
		if err != nil {
			return nil, xerrors.Errorf("unable to create parser for partition source: %w", err)
		}
		if resourceable, ok := parser.(parsers_resources.Resourceable); ok {
			resourceable.ResourcesObj().RunWatcher()
			rollbacks.Add(resourceable.ResourcesObj().Close)
		}
	}

	instanceCfgCopy := *cfg
	instanceCfgCopy.Instance = LogbrokerInstance(partition.Cluster)
	topicSourceCfg := instanceCfgCopy.buildTopicSourceConfig()

	source, err := topicapisource.NewPartitionSource(
		topicSourceCfg,
		topicapisource.PartitionDescription{
			Topic:     partition.Topic,
			Partition: int64(partition.Partition),
		},
		parser,
		logger,
		sourceMetrics,
	)
	if err != nil {
		return nil, xerrors.Errorf("unable to create TopicAPI partition source: %w", err)
	}

	rollbacks.Cancel()

	return source, nil
}
