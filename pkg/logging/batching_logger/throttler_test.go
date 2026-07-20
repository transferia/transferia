package batching_logger

import (
	"strings"
	"sync"
	"testing"
	"time"

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

func TestIntervalThrottler(t *testing.T) {
	interval := 10 * time.Millisecond
	fakeNow := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	intervalThrottler := NewIntervalThrottler(interval)
	intervalThrottler.nowFunc = func() time.Time { return fakeNow }

	myStr0 := ""
	LogLine(intervalThrottler, func(in string) { myStr0 = in }, "blablabla")
	require.True(t, strings.Contains(myStr0, "blablabla"))

	// not enough time has passed — still within the interval
	myStr1 := ""
	LogLine(intervalThrottler, func(in string) { myStr1 = in }, "blablabla")
	require.True(t, strings.Contains(myStr1, "skipped") && strings.Contains(myStr1, "by throttler"))

	// advance virtual clock past the interval
	fakeNow = fakeNow.Add(2 * interval)

	myStr2 := ""
	LogLine(intervalThrottler, func(in string) { myStr2 = in }, "blablabla")
	require.True(t, strings.Contains(myStr2, "blablabla"))
}

func TestIntervalThrottlerTSConcurrent(t *testing.T) {
	interval := 10 * time.Millisecond
	fakeNow := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	inner := NewIntervalThrottler(interval)
	inner.nowFunc = func() time.Time { return fakeNow }
	throttler := NewConcurrentThrottler(inner)

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	allowed := make([]bool, goroutines)
	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			allowed[i] = throttler.TryLog()
		}()
	}
	wg.Wait()

	allowedCount := 0
	for _, a := range allowed {
		if a {
			allowedCount++
		}
	}
	// TryLog atomically checks and marks under a single mutex lock,
	// so exactly one goroutine wins regardless of clock.
	require.Equal(t, 1, allowedCount, "only one goroutine should be allowed")
}

func TestSilentThrottler(t *testing.T) {
	// non-silent: emits the "skipped by throttler" line as before
	nonSilent := NewOnceThrottler()

	myStr0 := ""
	LogLine(nonSilent, func(in string) { myStr0 = in }, "blablabla")
	require.True(t, strings.Contains(myStr0, "blablabla"))

	myStr1 := ""
	LogLine(nonSilent, func(in string) { myStr1 = in }, "blablabla")
	require.True(t, strings.Contains(myStr1, "skipped") && strings.Contains(myStr1, "by throttler"))

	// silent: the logging func is never called on a throttled line
	silent := NewSilentThrottler(NewOnceThrottler())

	myStr2 := ""
	LogLine(silent, func(in string) { myStr2 = in }, "blablabla")
	require.True(t, strings.Contains(myStr2, "blablabla"))

	called := false
	myStr3 := ""
	LogLine(silent, func(in string) { called = true; myStr3 = in }, "blablabla")
	require.False(t, called, "silent throttler must not invoke the logging func when throttled")
	require.False(t, strings.Contains(myStr3, "skipped"))
	require.True(t, silent.IsSilent())
	require.False(t, nonSilent.IsSilent())
}

func TestUUID(t *testing.T) {
	onceThrottler := NewOnceThrottler()

	myStrArr := make([]string, 0)
	LogLine(onceThrottler, func(in string) { myStrArr = append(myStrArr, in) }, strings.Repeat("a", maxStringLengthConst)+"blablabla")
	for _, str := range myStrArr {
		require.True(t, strings.Contains(str, "[uuid:"))
	}
}
