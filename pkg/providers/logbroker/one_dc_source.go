package logbroker

import (
	"slices"
	"time"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/config/env"
	"github.com/transferia/transferia/pkg/parsers"
	parsers_resources "github.com/transferia/transferia/pkg/parsers/resources"
	topiccommon "github.com/transferia/transferia/pkg/providers/ydb/topics/common"
	topicsource "github.com/transferia/transferia/pkg/providers/ydb/topics/source"
	pqv1source "github.com/transferia/transferia/pkg/providers/ydb/topics/source/pqv1"
	topicapisource "github.com/transferia/transferia/pkg/providers/ydb/topics/source/topicapi"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

func newOneDCSource(cfg *LfSource, logger log.Logger, registry core_metrics.Registry) (abstract.Source, error) {
	// In test we use logbroker with local environment therefore we should skip this check
	if !env.IsTest() {
		if instanceIsValid := checkInstanceValidity(cfg.Instance); !instanceIsValid {
			return nil, abstract.NewFatalError(xerrors.Errorf("the instance '%s' from config is not known", cfg.Instance))
		}
	}

	sourceMetrics := stats.NewSourceStats(registry)
	parser, err := parsers.NewParserFromMap(cfg.ParserConfig, false, logger, sourceMetrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to make parser, err: %w", err)
	}
	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()
	if resourceable, ok := parser.(parsers_resources.Resourceable); ok {
		resourceable.ResourcesObj().RunWatcher()
		rollbacks.Add(resourceable.ResourcesObj().Close)
	}

	topicSourceCfg := &topicsource.Config{
		Connection: topiccommon.ConnectionConfig{
			Endpoint:    topiccommon.FormatEndpoint(string(cfg.Instance), cfg.Port),
			Database:    cfg.db(),
			Credentials: cfg.Credentials,
			TLSEnabled:  cfg.TLS == EnabledTLS,
			RootCAFiles: cfg.RootCAFiles,
		},

		Topics:   cfg.Topics,
		Consumer: cfg.Consumer,
		ReaderOpts: topicsource.ReaderOptions{
			ReadOnlyLocal:       cfg.Cluster != "" || cfg.OnlyLocal,
			MaxMemory:           int(cfg.MaxMemory),
			MaxReadSize:         uint32(cfg.MaxReadSize),
			MaxReadMessageCount: cfg.MaxReadMessagesCount,
			MaxTimeLag:          cfg.MaxTimeLag,
			MinReadInterval:     time.Millisecond * 95,
		},
		Transformer: nil,

		IsYDBTopicSink:             cfg.IsLbSink,
		AllowTTLRewind:             cfg.AllowTTLRewind,
		ParseQueueParallelism:      cfg.ParseQueueParallelism,
		UseFullTopicNameForParsing: true,
	}

	var source abstract.Source
	if cfg.UseTopicAPI {
		source, err = topicapisource.NewSource(topicSourceCfg, parser, logger, sourceMetrics)
		if err != nil {
			return nil, xerrors.Errorf("unable to create TopicAPI source: %w", err)
		}
	} else {
		source, err = pqv1source.NewSource(topicSourceCfg, parser, logger, sourceMetrics)
		if err != nil {
			return nil, xerrors.Errorf("unable to create pqv1 source: %w", err)
		}
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
