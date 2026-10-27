package logbroker

import (
	"context"

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

// newOneDCSource builds a source for a single logbroker endpoint. The passed ctx is
// forwarded to the pqv1 reader so that an in-progress connection attempt to an
// unreachable endpoint can be interrupted by canceling ctx, instead of blocking
// (potentially forever, since the pqv1 SDK retries indefinitely on its own) until the
// endpoint becomes reachable.
func newOneDCSource(ctx context.Context, cfg *LfSource, logger log.Logger, registry core_metrics.Registry) (abstract.Source, error) {
	// Explicit instances retain catalog validation; cluster readers use resolved
	// addresses. Tests may connect to a local instance outside the catalog.
	if !env.IsTest() && cfg.Cluster == "" {
		if !checkInstanceValidity(cfg.Instance) {
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

	topicSourceCfg, err := cfg.buildTopicSourceConfig()
	if err != nil {
		return nil, abstract.NewFatalError(xerrors.Errorf("unable to build topic source config: %w", err))
	}

	var source abstract.Source
	if cfg.UseTopicAPI {
		source, err = topicapisource.NewSource(topicSourceCfg, parser, logger, sourceMetrics)
		if err != nil {
			return nil, xerrors.Errorf("unable to create TopicAPI source: %w", err)
		}
	} else {
		source, err = pqv1source.NewSource(ctx, topicSourceCfg, parser, logger, sourceMetrics)
		if err != nil {
			return nil, xerrors.Errorf("unable to create pqv1 source: %w", err)
		}
	}

	rollbacks.Cancel()

	return source, nil
}

func checkInstanceValidity(configInstance LogbrokerInstance) bool {
	_, ok := installationByInstance(configInstance)
	return ok
}
