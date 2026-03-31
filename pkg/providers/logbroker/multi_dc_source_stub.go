//go:build !persqueue

package logbroker

import (
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

var (
	KnownClusters = map[LogbrokerCluster][]LogbrokerInstance{}
)

func isPersqueueTemporaryError(_ error) bool {
	return false
}

func NewMultiDCSource(_ *LfSource, _ log.Logger, _ core_metrics.Registry) (abstract.Source, error) {
	return nil, xerrors.New("persqueue support is not enabled: build with -tags persqueue")
}
