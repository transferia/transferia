package abstract

import "time"

type RegularSnapshot struct {
	Enabled               bool               `json:"Enabled" yaml:"enabled"`
	Interval              time.Duration      `json:"Interval" yaml:"interval"`
	CronExpression        string             `json:"CronExpression" yaml:"cron_expression"`
	IncrementDelaySeconds int64              `json:"IncrementDelaySeconds" yaml:"increment_delay_seconds"`
	Incremental           []IncrementalTable `json:"Incremental" yaml:"incremental"`
	RetryConfig           *RetryConfig       `json:"RetryConfig" yaml:"retry_config"`
}

type IncrementalTable struct {
	Name         string `yaml:"name"`
	Namespace    string `yaml:"namespace"`
	CursorField  string `yaml:"cursor_field"`
	InitialState string `yaml:"initial_state"`
}

type RetryConfig struct {
	MaxAttempts int64 `yaml:"max_attempts"`
}

func (t IncrementalTable) Initialized() bool {
	return t.CursorField != "" && t.InitialState != ""
}

func (t IncrementalTable) TableID() TableID {
	return TableID{Name: t.Name, Namespace: t.Namespace}
}
