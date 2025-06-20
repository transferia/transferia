//go:build !disable_delta_provider

package action

type JobInfo struct {
	JobID       string `json:"jobId,omitempty"`
	JobName     string `json:"jobName,omitempty"`
	RunID       string `json:"runId,omitempty"`
	JobOwnerID  string `json:"jobOwnerId,omitempty"`
	TriggerType string `json:"triggerType,omitempty"`
}
