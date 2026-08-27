package logbroker

import (
	"slices"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/config/env"
	"github.com/transferia/transferia/pkg/parsers"
	parsers_resources "github.com/transferia/transferia/pkg/parsers/resources"
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

	topicSourceCfg := cfg.buildTopicSourceConfig()

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
