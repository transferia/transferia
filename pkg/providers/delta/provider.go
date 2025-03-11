package delta

import (
	"encoding/gob"

	"github.com/transferria/transferria/library/go/core/metrics"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers"
	"go.ytsaurus.tech/library/go/core/log"
)

const ProviderType = abstract.ProviderType("delta")

func init() {
	sourceFactory := func() model.Source {
		return new(DeltaSource)
	}

	gob.Register(new(DeltaSource))
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
