package targets

import (
	"github.com/transferria/transferria/library/go/core/metrics"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/base"
	"github.com/transferria/transferria/pkg/providers"
	"go.ytsaurus.tech/library/go/core/log"
)

var UnknownTargetError = xerrors.New("unknown event target for destination, try legacy sinker instead")

func NewTarget(transfer *model.Transfer, lgr log.Logger, mtrcs metrics.Registry, cp coordinator.Coordinator, opts ...abstract.SinkOption) (t base.EventTarget, err error) {
	if factory, ok := providers.Destination[providers.Abstract2Sinker](lgr, mtrcs, cp, transfer); ok {
		return factory.Target(opts...)
	}
	return nil, UnknownTargetError
}
