package solomon

import (
	"encoding/json"
	"time"

	"go.uber.org/atomic"
)

var _ Metric = (*FuncCounter)(nil)

// FuncCounter tracks int64 value returned by function.
type FuncCounter struct {
	name       string
	metricType metricType
	tags       map[string]string
	function   func() int64
	timestamp  *time.Time

	useNameTag bool
	memOnly    bool
}

func (c *FuncCounter) getID() string {
	if c.timestamp != nil {
		return c.name + "(" + c.timestamp.Format(time.RFC3339) + ")"
	}
	return c.name
}

func (c *FuncCounter) Name() string {
	return c.name
}

func (c *FuncCounter) Function() func() int64 {
	return c.function
}

func (c *FuncCounter) getType() metricType {
	return c.metricType
}

func (c *FuncCounter) getLabels() map[string]string {
	return c.tags
}

func (c *FuncCounter) getValue() interface{} {
	return c.function()
}

func (c *FuncCounter) getTimestamp() *time.Time {
	return c.timestamp
}

func (c *FuncCounter) getNameTag() string {
	if c.useNameTag {
		return "name"
	} else {
		return "sensor"
	}
}

func (c *FuncCounter) isMemOnly() bool {
	return c.memOnly
}

func (c *FuncCounter) setMemOnly() {
	c.memOnly = true
}

// MarshalJSON implements json.Marshaler.
func (c *FuncCounter) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type      string            `json:"type"`
		Labels    map[string]string `json:"labels"`
		Value     int64             `json:"value"`
		Timestamp *int64            `json:"ts,omitempty"`
		MemOnly   bool              `json:"memOnly,omitempty"`
	}{
		Type:  c.metricType.String(),
		Value: c.function(),
		Labels: func() map[string]string {
			labels := make(map[string]string, len(c.tags)+1)
			labels[c.getNameTag()] = c.name
			for k, v := range c.tags {
				labels[k] = v
			}
			return labels
		}(),
		Timestamp: tsAsRef(c.timestamp),
		MemOnly:   c.memOnly,
	})
}

// Snapshot returns independent copy on metric.
func (c *FuncCounter) Snapshot() Metric {
	return &Counter{
		name:       c.name,
		metricType: c.metricType,
		tags:       c.tags,
		value:      *atomic.NewInt64(c.function()),

		useNameTag: c.useNameTag,
		memOnly:    c.memOnly,
	}
}
