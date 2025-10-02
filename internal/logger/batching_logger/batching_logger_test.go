package batching_logger

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/library/go/core/log"
)

type testEntry struct {
	level string
	msg   string
}

type testLogger struct {
	entries []testEntry
}

func newTestLogger() *testLogger { return &testLogger{} }

// Implement log.Logger and related interfaces
func (t *testLogger) Logger() log.Logger              { return t }
func (t *testLogger) Fmt() log.Fmt                    { return t }
func (t *testLogger) Structured() log.Structured      { return t }
func (t *testLogger) WithName(name string) log.Logger { return t }

func (t *testLogger) append(level, msg string) {
	t.entries = append(t.entries, testEntry{level: level, msg: msg})
}

// Structured
func (t *testLogger) Trace(msg string, fields ...log.Field) { t.append("TRACE", msg) }
func (t *testLogger) Debug(msg string, fields ...log.Field) { t.append("DEBUG", msg) }
func (t *testLogger) Info(msg string, fields ...log.Field)  { t.append("INFO", msg) }
func (t *testLogger) Warn(msg string, fields ...log.Field)  { t.append("WARN", msg) }
func (t *testLogger) Error(msg string, fields ...log.Field) { t.append("ERROR", msg) }
func (t *testLogger) Fatal(msg string, fields ...log.Field) { t.append("FATAL", msg) }

// Fmt
func (t *testLogger) Tracef(format string, args ...interface{}) {
	t.Trace(fmt.Sprintf(format, args...))
}
func (t *testLogger) Debugf(format string, args ...interface{}) {
	t.Debug(fmt.Sprintf(format, args...))
}
func (t *testLogger) Infof(format string, args ...interface{}) { t.Info(fmt.Sprintf(format, args...)) }
func (t *testLogger) Warnf(format string, args ...interface{}) { t.Warn(fmt.Sprintf(format, args...)) }
func (t *testLogger) Errorf(format string, args ...interface{}) {
	t.Error(fmt.Sprintf(format, args...))
}
func (t *testLogger) Fatalf(format string, args ...interface{}) {
	t.Fatal(fmt.Sprintf(format, args...))
}

func countEntries(entries []testEntry, level, msg string) int {
	c := 0
	for _, e := range entries {
		if e.level == level && e.msg == msg {
			c++
		}
	}
	return c
}

func findEntryContains(entries []testEntry, level, substr string) bool {
	for _, e := range entries {
		if e.level == level && strings.Contains(e.msg, substr) {
			return true
		}
	}
	return false
}

func waitUntil(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

func summaryLevel(level string) string {
	if level == "FATAL" {
		return "ERROR"
	}
	return level
}

type loggerFunctionWrapper interface {
	loggerFunction(msg string, fields ...log.Field)
}

type loggerFunction func(msg string, fields ...log.Field)

func (f loggerFunction) loggerFunction(msg string, fields ...log.Field) {
	f(msg, fields...)
}

type loggerFunctionFmt func(format string, args ...interface{})

func (f loggerFunctionFmt) loggerFunction(msg string, fields ...log.Field) {
	f(msg)
}

func allLoggerFunctions(b *BatchingLogger) (map[string]loggerFunction, map[string]loggerFunctionFmt) {
	allLoggerFunctions := map[string]loggerFunction{
		"DEBUG": b.Debug,
		"INFO":  b.Info,
		"WARN":  b.Warn,
		"ERROR": b.Error,
		"FATAL": b.Fatal,
		"TRACE": b.Trace,
	}
	allLoggerFunctionsFmt := map[string]loggerFunctionFmt{
		"DEBUG": b.Debugf,
		"INFO":  b.Infof,
		"WARN":  b.Warnf,
		"ERROR": b.Errorf,
		"FATAL": b.Fatalf,
		"TRACE": b.Tracef,
	}

	return allLoggerFunctions, allLoggerFunctionsFmt
}

func allLoggerFunctionsWrapper(b *BatchingLogger) map[string]loggerFunction {
	loggerFunctions, loggerFunctionsFmt := allLoggerFunctions(b)
	loggerFunctionsWrapper := make(map[string]loggerFunction)
	for k, v := range loggerFunctions {
		loggerFunctionsWrapper[k] = v.loggerFunction
	}
	for k, v := range loggerFunctionsFmt {
		loggerFunctionsWrapper[k] = v.loggerFunction
	}
	return loggerFunctionsWrapper
}

func TestBatchingLogger_SuppressesAndSummarizesDebug(t *testing.T) {
	tl := newTestLogger()
	b := NewBatchingLogger(tl, &BatchingOptions{FlushInterval: 50 * time.Millisecond, Threshold: 3})
	loggerFunctions := allLoggerFunctionsWrapper(b)

	for loggerFunctionName, loggerFunction := range loggerFunctions {
		for range 10 {
			loggerFunction("hello")
		}

		// Expect only first 3 immediate debug logs
		require.Equal(t, 3, countEntries(tl.entries, loggerFunctionName, "hello"))

		// Wait for summary to appear (note: fatal summaries are logged at ERROR)
		lvl := summaryLevel(loggerFunctionName)
		require.True(t, waitUntil(func() bool {
			return findEntryContains(tl.entries, lvl, "got 7 messages: hello")
		}, 2*time.Second))
	}
}

func TestBatchingLogger_NoSummaryWhenBelowThreshold(t *testing.T) {
	tl := newTestLogger()
	b := NewBatchingLogger(tl, &BatchingOptions{FlushInterval: 50 * time.Millisecond, Threshold: 3})
	loggerFunctions := allLoggerFunctionsWrapper(b)

	for loggerFunctionName, loggerFunction := range loggerFunctions {
		loggerFunction("a")
		loggerFunction("a")
		loggerFunction("b")
		loggerFunction("b")

		require.Equal(t, 2, countEntries(tl.entries, loggerFunctionName, "a"))
		require.Equal(t, 2, countEntries(tl.entries, loggerFunctionName, "b"))

		// Wait past one flush and ensure no summary produced
		time.Sleep(120 * time.Millisecond)
		lvl := summaryLevel(loggerFunctionName)
		require.False(t, findEntryContains(tl.entries, lvl, "got "))
	}
}

func TestBatchingLogger_WithNameSharesAggregator(t *testing.T) {
	tl := newTestLogger()
	b := NewBatchingLogger(tl, &BatchingOptions{FlushInterval: 50 * time.Millisecond, Threshold: 3})
	child := b.WithName("child").(*BatchingLogger)

	allParentLoggerFunctions := allLoggerFunctionsWrapper(b)
	allChildLoggerFunctions := allLoggerFunctionsWrapper(child)

	for loggerFunctionName, loggerFunction := range allParentLoggerFunctions {
		// 2 from parent, 2 from child -> 4 total; threshold=3 -> 3 immediate, 1 suppressed
		loggerFunction("x")
		loggerFunction("x")
		allChildLoggerFunctions[loggerFunctionName]("x")
		allChildLoggerFunctions[loggerFunctionName]("x")

		require.Equal(t, 3, countEntries(tl.entries, loggerFunctionName, "x"))
		lvl := summaryLevel(loggerFunctionName)
		require.True(t, waitUntil(func() bool { return findEntryContains(tl.entries, lvl, "got 1 messages: x") }, 2*time.Second))
	}
}

func TestBatchingLogger_DifferentLogs(t *testing.T) {
	tl := newTestLogger()
	b := NewBatchingLogger(tl, &BatchingOptions{FlushInterval: 50 * time.Millisecond, Threshold: 3, MaxKeys: 50})

	for i := 1; i < 100; i++ {
		b.Info(strings.Repeat("x", i))
	}

	for i := 1; i < 100; i++ {
		got := countEntries(tl.entries, "INFO", strings.Repeat("x", i))
		require.Equal(t, 1, got, "expected %d immediate INFO '%s', got %d", i, strings.Repeat("x", i), got)
	}

	require.Equal(t, 50, len(b.aggr.counts))
}

func TestBatchingLogger_LongSpamLog(t *testing.T) {
	tl := newTestLogger()
	flushInterval := 50 * time.Millisecond
	b := NewBatchingLogger(tl, &BatchingOptions{FlushInterval: flushInterval, Threshold: 3})

	summarizedCntLogged := 0
	for i := 0; i < 10; i++ {
		for !b.aggr.willBeFlushed.CompareAndSwap(false, true) {
		} // wait for flush to be started
		for j := 0; j < 5; j++ {
			b.Info("some log")
			summarizedCntLogged++
		}
		b.aggr.willBeFlushed.Store(false)
		b.Info("some log")
		summarizedCntLogged++
	}

	for b.aggr.willBeFlushed.Load() {
	} // wait for flush to be finished

	require.Equal(t, 3, countEntries(tl.entries, "INFO", "some log"))
	require.Equal(t, 1, countEntries(tl.entries, "INFO", "got 3 messages: some log"))
	require.Equal(t, 9, countEntries(tl.entries, "INFO", "got 6 messages: some log"))

	loggedCnt := 0
	for _, e := range tl.entries {
		require.Equal(t, "INFO", e.level)
		require.Contains(t, e.msg, "some log")

		if strings.Contains(e.msg, "got") {
			parts := strings.Split(e.msg, " ")
			cnt, err := strconv.Atoi(parts[1])
			require.NoError(t, err)
			loggedCnt += cnt
		} else {
			loggedCnt++
		}
	}
	require.Equal(t, summarizedCntLogged, loggedCnt)

	time.Sleep(2 * flushInterval)
	b.Info("log for starting flush")
	time.Sleep(flushInterval)
	require.Equal(t, 1, countEntries(tl.entries, "INFO", "log for starting flush"))
	require.False(t, b.aggr.willBeFlushed.Load())
	require.Nil(t, b.aggr.counts[classification{level: levelInfo, key: "some log"}])
	require.Len(t, b.aggr.counts, 1)
	require.NotNil(t, b.aggr.counts[classification{level: levelInfo, key: "log for starting flush"}])
}
