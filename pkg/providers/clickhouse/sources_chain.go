//go:build !disable_clickhouse_provider

package clickhouse

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type SourcesChain struct {
	sources []base.EventSource
	logger  log.Logger
}

func (p *SourcesChain) Progress() (base.EventSourceProgress, error) {
	for _, source := range p.sources {
		if progressable, ok := source.(base.ProgressableEventSource); ok {
			return progressable.Progress()
		}
	}
	return nil, xerrors.New("progressable event source not found in chain")
}

func (p *SourcesChain) Running() bool {
	for _, source := range p.sources {
		if source.Running() {
			return true
		}
	}
	return false
}

func (p *SourcesChain) Start(ctx context.Context, target base.EventTarget) error {
	for _, source := range p.sources {
		if err := source.Start(ctx, target); err != nil {
			return xerrors.Errorf("unable to start %T event source: %w", source, err)
		}
		p.logger.Infof("source completed: %T", source)
	}
	return nil
}

func (p *SourcesChain) Stop() error {
	var errs util.Errors
	for _, provider := range p.sources {
		if err := provider.Stop(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return xerrors.Errorf("unable to stop sources chain: %w", errs)
	}
	return nil
}

func NewSourcesChain(logger log.Logger, sources ...base.EventSource) base.ProgressableEventSource {
	return &SourcesChain{
		sources: sources,
		logger:  logger,
	}
}
