package sequencer

import (
	"fmt"
	"strings"
)

// BuildMapPartitionToOffsetsRange - is used only in logging
func BuildMapPartitionToOffsetsRange(messages []QueueMessage) string {
	sequencer := NewSequencer()
	_ = sequencer.StartProcessing(messages)
	return sequencer.ToStringRanges()
}

// BuildPartitionOffsetLogLine - is used only in logging
func BuildPartitionOffsetLogLine(messages []QueueMessage) string {
	if len(messages) == 0 {
		return ""
	}
	result := ""
	for _, message := range messages {
		result += fmt.Sprintf("%d:%d,", message.Partition, message.Offset)
	}
	return result[0 : len(result)-1]
}

// BuildMapTopicPartitionToOffsetsRange - is used only in logging
func BuildMapTopicPartitionToOffsetsRange(messages []QueueMessage) string {
	sequencer := NewSequencer()
	_ = sequencer.StartProcessing(messages)
	return sequencer.ToStringRangesWithTopic()
}

// OffsetsToRanges converts a sorted slice of int64 offsets into a compact range string.
// Consecutive offsets are collapsed into "start-end", non-consecutive ones are comma-separated.
// Example: [1, 2, 3, 5, 6, 8] -> "1-3,5-6,8"
func OffsetsToRanges(offsets []int64) string {
	if len(offsets) == 0 {
		return ""
	}

	var b strings.Builder
	rangeBegin := offsets[0]
	rangeEnd := offsets[0]
	inRange := false

	for _, curr := range offsets[1:] {
		if rangeEnd+1 == curr {
			rangeEnd = curr
			inRange = true
			continue
		}
		if inRange {
			_, _ = fmt.Fprintf(&b, "%d-%d,", rangeBegin, rangeEnd)
		} else {
			_, _ = fmt.Fprintf(&b, "%d,", rangeBegin)
		}
		rangeBegin = curr
		rangeEnd = curr
		inRange = false
	}

	if inRange {
		_, _ = fmt.Fprintf(&b, "%d-%d", rangeBegin, rangeEnd)
	} else {
		_, _ = fmt.Fprintf(&b, "%d", rangeBegin)
	}
	return b.String()
}
