//go:build !disable_delta_provider

package delta

import (
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers"
	"github.com/transferia/transferia/pkg/util/gobwrapper"
	"go.ytsaurus.tech/library/go/core/log"
)

const ProviderType = abstract.ProviderType("delta")

func init() {
	sourceFactory := func() model.Source {
		return new(DeltaSource)
	}

	gobwrapper.Register(new(DeltaSource))
	model.RegisterSource(ProviderType, sourceFactory)
	abstract.RegisterProviderName(ProviderType, "Delta Lake")
}

// To verify providers contract implementation
var (
	_ providers.Snapshot = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	transfer *model.Transfer
}

func (p Provider) Type() abstract.ProviderType {
	return ProviderType
}

func (p Provider) Storage() (abstract.Storage, error) {
	src, ok := p.transfer.Src.(*DeltaSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected src type: %T", p.transfer.Src)
	}

	return NewStorage(src, p.logger, p.registry)
}
