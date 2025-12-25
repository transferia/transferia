package targets

import (
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/providers"
	"go.ytsaurus.tech/library/go/core/log"
)

var UnknownTargetError = xerrors.New("unknown event target for destination, try legacy sinker instead")

func NewTarget(transfer *model.Transfer, lgr log.Logger, mtrcs metrics.Registry, cp coordinator.Coordinator, opts ...abstract.SinkOption) (t base.EventTarget, err error) {
	if factory, ok := providers.Destination[providers.Abstract2Sinker](lgr, mtrcs, cp, transfer); ok {
		return factory.Target(opts...)
	}
	return nil, UnknownTargetError
}
