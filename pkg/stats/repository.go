package stats

import (
	"time"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
)

type RepositoryStat struct {
	DecodeTransferWithEndpointsError metrics.Counter
}

func NewRepositoryStat() *RepositoryStat {
	registry := solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()})
	return &RepositoryStat{
		DecodeTransferWithEndpointsError: registry.Counter("controlplane.repository.decode.transfer_with_endpoints.error"),
	}
}
