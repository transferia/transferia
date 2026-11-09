//go:build !cgo
// +build !cgo

package datadog

import (
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewSink(_ *DatadogDestination, _ log.Logger, _ core_metrics.Registry) (abstract.Sinker, error) {
	return nil, xerrors.New("Datadog provider is not presented in this build")
}
