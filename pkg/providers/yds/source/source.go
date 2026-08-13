package source

import (
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	parsers_resources "github.com/transferia/transferia/pkg/parsers/resources"
	pqv1source "github.com/transferia/transferia/pkg/providers/ydb/topics/source/pqv1"
	topicapisource "github.com/transferia/transferia/pkg/providers/ydb/topics/source/topicapi"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewSourceWithParser(transferID string, cfg *YDSSource, logger log.Logger, sourceMetrics *stats.SourceStats, parser parsers.Parser) (abstract.Source, error) {
	return pqv1source.NewSource(cfg.topicSourceConfig(transferID), parser, logger, sourceMetrics)
}

func NewSource(transferID string, cfg sourceConfig, logger log.Logger, registry metrics.Registry) (abstract.Source, error) {
	sourceMetrics := stats.NewSourceStats(registry)
	switch c := cfg.(type) {
	case *YDSSource:
		source, err := newYDSSource(transferID, c, logger, sourceMetrics)
		if err != nil {
			return nil, xerrors.Errorf("unable to create YDS source: %w", err)
		}
		return source, nil
	case *YDBTopicSource:
		source, err := newYDBTopicSource(c, logger, sourceMetrics)
		if err != nil {
			return nil, xerrors.Errorf("unable to create YDB Topic source: %w", err)
		}
		return source, nil
	default:
		return nil, xerrors.Errorf("unsupported YDS source config type: %T", cfg)
	}
}

func newYDSSource(transferID string, cfg *YDSSource, logger log.Logger, sourceMetrics *stats.SourceStats) (abstract.Source, error) {
	if err := resolveCredentialsIfNeeded(cfg, logger); err != nil {
		return nil, xerrors.Errorf("unable to resolve credentials: %w", err)
	}

	parser, err := createParser(cfg, logger, sourceMetrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to create parser: %w", err)
	}

	return pqv1source.NewSource(cfg.topicSourceConfig(transferID), parser, logger, sourceMetrics)
}

func newYDBTopicSource(cfg *YDBTopicSource, logger log.Logger, sourceMetrics *stats.SourceStats) (abstract.Source, error) {
	parser, err := parsers.NewParserFromMap(cfg.ParserConfig, false, logger, sourceMetrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to make parser: %w", err)
	}

	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()
	if resourceable, ok := parser.(parsers_resources.Resourceable); ok {
		resourceable.ResourcesObj().RunWatcher()
		rollbacks.Add(resourceable.ResourcesObj().Close)
	}

	source, err := topicapisource.NewSource(cfg.topicSourceConfig(""), parser, logger, sourceMetrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to create topic source: %w", err)
	}
	rollbacks.Cancel()

	return source, nil
}
