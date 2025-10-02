package batching_logger

import (
	"fmt"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
)

type levelKind int8

const (
	levelTrace levelKind = iota
	levelDebug
	levelInfo
	levelWarn
	levelError
	levelFatal
)

var _ log.Logger = (*BatchingLogger)(nil)

// BatchingLogger is a logger that batches logs and sends them to the logger
type BatchingLogger struct {
	logger log.Logger
	aggr   *spamAggregator
}

// BatchingOptions is a set of options for the batching logger
type BatchingOptions struct {
	FlushInterval time.Duration
	Threshold     int
	MaxKeys       int
}

func NewBatchingLogger(l log.Logger, opts *BatchingOptions) *BatchingLogger {
	opt := defaultAggregatorConfig()
	if opts != nil {
		if opts.FlushInterval > 0 {
			opt.flushInterval = opts.FlushInterval
		}
		if opts.Threshold > 0 {
			opt.threshold = opts.Threshold
		}
		if opts.MaxKeys > 0 {
			opt.maxKeys = opts.MaxKeys
		}
	}
	return &BatchingLogger{
		logger: l,
		aggr:   newSpamAggregator(l, opt),
	}
}

func (b *BatchingLogger) Logger() log.Logger { return b }

func (b *BatchingLogger) Fmt() log.Fmt { return b }

func (b *BatchingLogger) Structured() log.Structured { return b }

func (b *BatchingLogger) Debug(msg string, fields ...log.Field) {
	if b.aggr == nil || b.aggr.shouldLog(levelDebug, msg) {
		b.logger.Debug(msg, fields...)
	}
}

func (b *BatchingLogger) Debugf(format string, args ...interface{}) {
	formatted := fmt.Sprintf(format, args...)
	if b.aggr == nil || b.aggr.shouldLog(levelDebug, formatted) {
		b.logger.Debug(formatted)
	}
}

func (b *BatchingLogger) Info(msg string, fields ...log.Field) {
	if b.aggr == nil || b.aggr.shouldLog(levelInfo, msg) {
		b.logger.Info(msg, fields...)
	}
}

func (b *BatchingLogger) Infof(format string, args ...interface{}) {
	formatted := fmt.Sprintf(format, args...)
	if b.aggr == nil || b.aggr.shouldLog(levelInfo, formatted) {
		b.logger.Info(formatted)
	}
}

func (b *BatchingLogger) Warn(msg string, fields ...log.Field) {
	if b.aggr == nil || b.aggr.shouldLog(levelWarn, msg) {
		b.logger.Warn(msg, fields...)
	}
}

func (b *BatchingLogger) Warnf(format string, args ...interface{}) {
	formatted := fmt.Sprintf(format, args...)
	if b.aggr == nil || b.aggr.shouldLog(levelWarn, formatted) {
		b.logger.Warn(formatted)
	}
}

func (b *BatchingLogger) Error(msg string, fields ...log.Field) {
	if b.aggr == nil || b.aggr.shouldLog(levelError, msg) {
		b.logger.Error(msg, fields...)
	}
}

func (b *BatchingLogger) Errorf(format string, args ...interface{}) {
	formatted := fmt.Sprintf(format, args...)
	if b.aggr == nil || b.aggr.shouldLog(levelError, formatted) {
		b.logger.Error(formatted)
	}
}

func (b *BatchingLogger) Fatal(msg string, fields ...log.Field) {
	if b.aggr == nil || b.aggr.shouldLog(levelFatal, msg) {
		b.logger.Fatal(msg, fields...)
	}
}

func (b *BatchingLogger) Fatalf(format string, args ...interface{}) {
	formatted := fmt.Sprintf(format, args...)
	if b.aggr == nil || b.aggr.shouldLog(levelFatal, formatted) {
		b.logger.Fatal(formatted)
	}
}

func (b *BatchingLogger) Trace(msg string, fields ...log.Field) {
	if b.aggr == nil || b.aggr.shouldLog(levelTrace, msg) {
		b.logger.Trace(msg, fields...)
	}
}

func (b *BatchingLogger) Tracef(format string, args ...interface{}) {
	formatted := fmt.Sprintf(format, args...)
	if b.aggr == nil || b.aggr.shouldLog(levelTrace, formatted) {
		b.logger.Trace(formatted)
	}
}

func (b *BatchingLogger) WithName(name string) log.Logger {
	return &BatchingLogger{
		logger: b.logger.WithName(name),
		aggr:   b.aggr,
	}
}
