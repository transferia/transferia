//go:build !disable_greenplum_provider

package greenplum

import "github.com/transferia/transferia/pkg/abstract"

const EtaRowPartialProgress = 1 << 20

// ComposePartialProgressFn allows to transform progress by part into total progress by multiple parts
func ComposePartialProgressFn(base abstract.LoadProgress, completedParts uint, totalParts uint, totalEta uint64) abstract.LoadProgress {
	return func(current uint64, progress uint64, total uint64) {
		inPartProgress := (float64(progress) / float64(total))
		if inPartProgress > 1.0 {
			inPartProgress = 1.0
		}
		integrator := float64(totalEta) / float64(totalParts)
		progressOfTotal := uint64((inPartProgress + float64(completedParts)) * integrator)
		base(current, progressOfTotal, totalEta)
	}
}
