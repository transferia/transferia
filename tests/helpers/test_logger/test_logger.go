package test_logger

import (
	"fmt"

	"go.ytsaurus.tech/library/go/core/log"
)

type TestEntry struct {
	Level string
	Msg   string
}

type TestLogger struct {
	Entries []TestEntry
}

func NewTestLogger() *TestLogger { return &TestLogger{} }

// Implement log.Logger and related interfaces
func (t *TestLogger) Logger() log.Logger              { return t }
func (t *TestLogger) Fmt() log.Fmt                    { return t }
func (t *TestLogger) Structured() log.Structured      { return t }
func (t *TestLogger) WithName(name string) log.Logger { return t }

func (t *TestLogger) append(level, msg string) {
	t.Entries = append(t.Entries, TestEntry{Level: level, Msg: msg})
}

// Structured
func (t *TestLogger) Trace(msg string, fields ...log.Field) { t.append("TRACE", msg) }
func (t *TestLogger) Debug(msg string, fields ...log.Field) { t.append("DEBUG", msg) }
func (t *TestLogger) Info(msg string, fields ...log.Field)  { t.append("INFO", msg) }
func (t *TestLogger) Warn(msg string, fields ...log.Field)  { t.append("WARN", msg) }
func (t *TestLogger) Error(msg string, fields ...log.Field) { t.append("ERROR", msg) }
func (t *TestLogger) Fatal(msg string, fields ...log.Field) { t.append("FATAL", msg) }

// Fmt
func (t *TestLogger) Tracef(format string, args ...interface{}) {
	t.Trace(fmt.Sprintf(format, args...))
}
func (t *TestLogger) Debugf(format string, args ...interface{}) {
	t.Debug(fmt.Sprintf(format, args...))
}
func (t *TestLogger) Infof(format string, args ...interface{}) { t.Info(fmt.Sprintf(format, args...)) }
func (t *TestLogger) Warnf(format string, args ...interface{}) { t.Warn(fmt.Sprintf(format, args...)) }
func (t *TestLogger) Errorf(format string, args ...interface{}) {
	t.Error(fmt.Sprintf(format, args...))
}
func (t *TestLogger) Fatalf(format string, args ...interface{}) {
	t.Fatal(fmt.Sprintf(format, args...))
}
