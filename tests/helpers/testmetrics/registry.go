package testmetrics

import (
	"time"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
)

func EmptyRegistry() metrics.Registry {
	return solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()})
}
