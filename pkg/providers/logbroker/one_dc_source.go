package logbroker

import (
	"slices"
	"time"

	"github.com/transferia/transferia/kikimr/public/sdk/go/persqueue"
	"github.com/transferia/transferia/kikimr/public/sdk/go/persqueue/log/corelogadapter"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/config/env"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/resources"
	ydssource "github.com/transferia/transferia/pkg/providers/yds/source"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/xtls"
	"go.ytsaurus.tech/library/go/core/log"
)

func newOneDCSource(cfg *LfSource, logger log.Logger, registry metrics.Registry) (abstract.Source, error) {
	// In test we use logbroker with local environment therefore we should skip this check
	if !env.IsTest() {
		if instanceIsValid := checkInstanceValidity(cfg.Instance); !instanceIsValid {
			return nil, abstract.NewFatalError(xerrors.Errorf("the instance '%s' from config is not known", cfg.Instance))
		}
	}

	var topics []persqueue.TopicInfo
	if len(cfg.Topics) > 0 {
		topics = make([]persqueue.TopicInfo, len(cfg.Topics))
		for i, topic := range cfg.Topics {
			topics[i] = persqueue.TopicInfo{
				Topic:           topic,
				PartitionGroups: nil,
			}
		}
	}

	var opts persqueue.ReaderOptions
	opts.Logger = corelogadapter.New(logger)
	opts.Endpoint = string(cfg.Instance)
	opts.Database = cfg.Database
	opts.ManualPartitionAssignment = true
	opts.Consumer = cfg.Consumer
	opts.Topics = topics
	opts.ReadOnlyLocal = cfg.Cluster != "" || cfg.OnlyLocal
	opts.MaxReadSize = uint32(cfg.MaxReadSize)
	opts.MaxMemory = int(cfg.MaxMemory)
	opts.MaxTimeLag = cfg.MaxTimeLag
	opts.RetryOnFailure = true
	opts.MaxReadMessagesCount = cfg.MaxReadMessagesCount
	opts.Port = cfg.Port
	opts.Credentials = cfg.Credentials
	opts.MinReadInterval = time.Millisecond * 95

	if cfg.TLS == EnabledTLS {
		var err error
		opts.TLSConfig, err = xtls.FromPath(cfg.RootCAFiles)
		if err != nil {
			return nil, xerrors.Errorf("unable to load tls: %w", err)
		}
	}

	sourceMetrics := stats.NewSourceStats(registry)
	parser, err := parsers.NewParserFromMap(cfg.ParserConfig, false, logger, sourceMetrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to make parser, err: %w", err)
	}
	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()
	if resourceable, ok := parser.(resources.Resourceable); ok {
		resourceable.ResourcesObj().RunWatcher()
		rollbacks.Add(resourceable.ResourcesObj().Close)
	}

	ydsCfg := ydsSourceConfig(cfg.AllowTTLRewind, cfg.IsLbSink, cfg.ParseQueueParallelism)

	// transferID is empty because it is used to specify the consumer, and it is already specified in the readerOpts
	transferID := ""
	source, err := ydssource.NewSourceWithOpts(transferID, ydsCfg, logger, registry,
		ydssource.WithCreds(cfg.Credentials),
		ydssource.WithReaderOpts(&opts),
		ydssource.WithUseFullTopicName(true),
		ydssource.WithParser(parser),
	)
	if err != nil {
		return nil, xerrors.Errorf("unable to create pqv1 source: %w", err)
	}

	rollbacks.Cancel()

	return source, nil
}

func checkInstanceValidity(configInstance LogbrokerInstance) bool {
	for _, knownInstances := range KnownClusters {
		if slices.Contains(knownInstances, configInstance) {
			return true
		}
	}
	return false
}
