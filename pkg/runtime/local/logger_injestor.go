package local

import (
	"github.com/transferria/transferria/internal/logger"
	"go.ytsaurus.tech/library/go/core/log"
)

// WithLogger temproray hack to injest global logger into dataplane
func WithLogger(lgr log.Logger) {
	logger.Log = log.With(lgr, log.Any("component", "dataplane"))
	logger.Log.Info("override logger inside data plane")
}
