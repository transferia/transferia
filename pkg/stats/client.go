package stats

import (
	"github.com/transferia/transferia/cloud/dataplatform/ycloud/grpcstats"
)

const (
	// Metric names
	MetricClientRequestCount    = "requests.by_code.count"
	MetricClientRequestDuration = "requests.by_duration.count"

	// Label names
	LabelMethod    = "method"
	LabelErrorCode = "error_code"
	LabelRole      = "role"

	// Label values
	RoleClient = "client"
)

var ClientStatsLabelsConfig = grpcstats.LabelsConfig{
	MetricNames: grpcstats.MetricNames{
		RequestCount:    MetricClientRequestCount,
		RequestDuration: MetricClientRequestDuration,
	},
	LabelNames: grpcstats.LabelNames{
		Method:    LabelMethod,
		ErrorCode: LabelErrorCode,
	},
}
