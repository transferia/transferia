package shared

import (
	"runtime/debug"

	"github.com/dustin/go-humanize"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/format"
)

const (
	memLimitReserveBytes = 200 * humanize.MiByte
	memLimitReservePerc  = 0.1
)

// calcMemLimit calculates value of GOMEMLIMIT - soft limit of RAM usage which should be respected by GC.
// Some amount of memory is reserved for unaccounted memory (e.g. CGO) and system needs
// and for algorithmic imperfections of the limiter implementation.
// The reserve amounts to memLimitReservePerc * 100 percents of RAM but at least memLimitReserveBytes
// See https://go.dev/doc/gc-guide#Memory_limit for futher details
func calcMemLimit(ram uint64) uint64 {
	if int(float64(ram)*memLimitReservePerc) < memLimitReserveBytes {
		// Reserve nothing for extremely low RAM values
		if ram < (memLimitReserveBytes * 2) {
			return ram
		}
		return ram - memLimitReserveBytes
	}
	return uint64(float64(ram) * (1 - memLimitReservePerc))
}

func applyMemoryLimit(ramLimit uint64) {
	if ramLimit <= 0 {
		logger.Log.Infof("no soft limit: %s", format.SizeInt(int(ramLimit)))
		return
	}
	logger.Log.Infof("apply soft limit: %s", format.SizeInt(int(ramLimit)))
	debug.SetMemoryLimit(int64(ramLimit))
}

func ApplyRuntimeLimits(rt abstract.Runtime) {
	if limiter, ok := rt.(abstract.LimitedResourceRuntime); ok && limiter.ResourceLimiterEnabled() {
		applyMemoryLimit(calcMemLimit(limiter.RAMGuarantee()))
		if limiter.GCPercentage() > 0 {
			logger.Log.Infof("apply gc percentage: %v", limiter.GCPercentage())
			debug.SetGCPercent(limiter.GCPercentage())
		}
	}
}
