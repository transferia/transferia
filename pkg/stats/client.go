package stats

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
