package source

import (
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	pqv1source "github.com/transferia/transferia/pkg/providers/ydb/topics/source/pqv1"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewSourceWithParser(transferID string, cfg *YDSSource, logger log.Logger, sourceMetrics *stats.SourceStats, parser parsers.Parser) (abstract.Source, error) {
	topicSourceCfg := buildTopicSourceConfig(transferID, cfg)
	return pqv1source.NewSource(topicSourceCfg, parser, logger, sourceMetrics)
}

func NewSource(transferID string, cfg *YDSSource, logger log.Logger, registry metrics.Registry) (abstract.Source, error) {
	if err := resolveCredentialsIfNeeded(cfg, logger); err != nil {
		return nil, xerrors.Errorf("unable to resolve credentials: %w", err)
	}

	sourceMetrics := stats.NewSourceStats(registry)
	parser, err := createParser(cfg, logger, sourceMetrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to create parser: %w", err)
	}

	return NewSourceWithParser(transferID, cfg, logger, sourceMetrics, parser)
}
