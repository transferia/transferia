package batching_logger

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOneThrottler(t *testing.T) {
	onceThrottler := NewOnceThrottler()

	myStr0 := ""
	LogLine(onceThrottler, func(in string) { myStr0 = in }, "blablabla")
	require.True(t, strings.Contains(myStr0, "blablabla"))

	myStr1 := ""
	LogLine(onceThrottler, func(in string) { myStr1 = in }, "blablabla")
	require.True(t, strings.Contains(myStr1, "skipped") && strings.Contains(myStr1, "by throttler"))
}

func TestCountThrottler(t *testing.T) {
	onceThrottler := NewCountThrottler(2)

	myStr0 := ""
	LogLine(onceThrottler, func(in string) { myStr0 = in }, "blablabla")
	require.True(t, strings.Contains(myStr0, "blablabla"))

	myStr1 := ""
	LogLine(onceThrottler, func(in string) { myStr1 = in }, "blablabla")
	require.True(t, strings.Contains(myStr1, "blablabla"))

	myStr2 := ""
	LogLine(onceThrottler, func(in string) { myStr2 = in }, "blablabla")
	require.True(t, strings.Contains(myStr2, "skipped") && strings.Contains(myStr2, "by throttler"))
}
