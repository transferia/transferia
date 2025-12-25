package stats

import (
	"github.com/transferia/transferia/library/go/core/metrics"
)

type StopperStats struct {
	TotalTransfers          metrics.IntGauge
	WarningLabeledTransfers metrics.IntGauge
	ErrorLabeledTransfers   metrics.IntGauge

	pm metrics.Registry
}

func NewStopperStats(mtrc metrics.Registry) *StopperStats {
	pm := mtrc.WithTags(map[string]string{
		"component": "regular_stopper",
	})
	return &StopperStats{
		TotalTransfers:          pm.IntGauge("transfers.total"),
		WarningLabeledTransfers: pm.IntGauge("transfers.warning_labels"),
		ErrorLabeledTransfers:   pm.IntGauge("transfers.error_labels"),
		pm:                      pm,
	}
}
