package model

import "strings"

// SystemLabel contains transfer label names which are reserved to control
// hidden experimental transfer features.
//
// Each SystemLabel value must be documented
// SystemLabel is set by dt automation to enable/disable certain features
type SystemLabel string

// FeatureLabel contains transfer label names which are reserved to control
// hidden experimental transfer features.

// Each FeatureLabel value must be documented
// FeatureLabel is set by users to enable/disable certain features
type FeatureLabel string

// SystemLabelPrefix for reference
const systemLabelPrefix = "dt.system."

// Regular snapshot monitoring labels for forced stop functionality
const (
	// LabelWarnRegularSnapshotStopCandidate is set when a regular snapshot transfer
	// has not had successful activations for an extended period and is a candidate for forced stop
	LabelWarnRegularSnapshotStopCandidate SystemLabel = SystemLabel(systemLabelPrefix + "warn.regular-snapshot-stop-candidate")

	// LabelErrRegularSnapshotStopped is set when a regular snapshot transfer
	// has been forcibly stopped due to prolonged inactivity
	LabelErrRegularSnapshotStopped SystemLabel = SystemLabel(systemLabelPrefix + "err.regular-snapshot-stopped")

	// FeatureLabelAsyncCH is used to enable experimental asynchronous clickhouse snapshot sink
	//
	// The only known value is "on", any other value means disabled.

	FeatureLabelAsyncCH = FeatureLabel("dt-async-ch")

	// FeatureLabelMemThrottle is used to enable/disable MemoryThrorrler middleware.
	//
	// The only known value is "on", any other value means disabled.
	FeatureLabelMemThrottle = FeatureLabel("dt-mem-throttle")
)

// IsSystemLabelKey returns true if the provided label key is a reserved system label
func IsSystemLabelKey(key string) bool {
	// legacy labels should be checked as well
	return strings.HasPrefix(key, systemLabelPrefix)
}
