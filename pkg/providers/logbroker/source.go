package logbroker

import (
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewSource(cfg *LfSource, logger log.Logger, registry metrics.Registry) (abstract.Source, error) {
	if cfg.Cluster != "" && len(KnownClusters[cfg.Cluster]) > 0 {
		result, err := NewMultiDCSource(cfg, logger, registry)
		if err != nil {
			return nil, xerrors.Errorf("unable to create multi-dc source, err: %w", err)
		}
		return result, nil
	}
	result, err := newOneDCSource(cfg, logger, registry)
	if err != nil {
		return nil, xerrors.Errorf("unable to create one-dc source, err: %w", err)
	}
	return result, nil
}
