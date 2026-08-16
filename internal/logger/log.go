package logger

import (
	"context"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
)

func Trace(ctx context.Context, msg string, fields ...log.Field) {
	ctxlog.Trace(ctx, log.AddCallerSkip(Log, 1), msg, fields...)
}

func TraceWithLogger(ctx context.Context, logger log.Logger, msg string, fields ...log.Field) {
	ctxlog.Trace(ctx, log.AddCallerSkip(logger, 1), msg, fields...)
}

func Debug(ctx context.Context, msg string, fields ...log.Field) {
	ctxlog.Debug(ctx, log.AddCallerSkip(Log, 1), msg, fields...)
}

func DebugWithLogger(ctx context.Context, logger log.Logger, msg string, fields ...log.Field) {
	ctxlog.Debug(ctx, log.AddCallerSkip(logger, 1), msg, fields...)
}

func Info(ctx context.Context, msg string, fields ...log.Field) {
	ctxlog.Info(ctx, log.AddCallerSkip(Log, 1), msg, fields...)
}

func InfoWithLogger(ctx context.Context, logger log.Logger, msg string, fields ...log.Field) {
	ctxlog.Info(ctx, log.AddCallerSkip(logger, 1), msg, fields...)
}

func Warn(ctx context.Context, msg string, fields ...log.Field) {
	ctxlog.Warn(ctx, log.AddCallerSkip(Log, 1), msg, fields...)
}

func WarnWithLogger(ctx context.Context, logger log.Logger, msg string, fields ...log.Field) {
	ctxlog.Warn(ctx, log.AddCallerSkip(logger, 1), msg, fields...)
}

func Error(ctx context.Context, msg string, fields ...log.Field) {
	ctxlog.Error(ctx, log.AddCallerSkip(Log, 1), msg, fields...)
}

func ErrorWithLogger(ctx context.Context, logger log.Logger, msg string, fields ...log.Field) {
	ctxlog.Error(ctx, log.AddCallerSkip(logger, 1), msg, fields...)
}

func Fatal(ctx context.Context, msg string, fields ...log.Field) {
	ctxlog.Fatal(ctx, log.AddCallerSkip(Log, 1), msg, fields...)
}

func FatalWithLogger(ctx context.Context, logger log.Logger, msg string, fields ...log.Field) {
	ctxlog.Fatal(ctx, log.AddCallerSkip(logger, 1), msg, fields...)
}

func Tracef(ctx context.Context, format string, args ...interface{}) {
	ctxlog.Tracef(ctx, log.AddCallerSkip(Log, 1), format, args...)
}

func TracefWithLogger(ctx context.Context, l log.Logger, format string, args ...interface{}) {
	ctxlog.Tracef(ctx, log.AddCallerSkip(l, 1), format, args...)
}

func Debugf(ctx context.Context, format string, args ...interface{}) {
	ctxlog.Debugf(ctx, log.AddCallerSkip(Log, 1), format, args...)
}

func DebugfWithLogger(ctx context.Context, l log.Logger, format string, args ...interface{}) {
	ctxlog.Debugf(ctx, log.AddCallerSkip(l, 1), format, args...)
}

func Infof(ctx context.Context, format string, args ...interface{}) {
	ctxlog.Infof(ctx, log.AddCallerSkip(Log, 1), format, args...)
}

func InfofWithLogger(ctx context.Context, l log.Logger, format string, args ...interface{}) {
	ctxlog.Infof(ctx, log.AddCallerSkip(l, 1), format, args...)
}

func Warnf(ctx context.Context, format string, args ...interface{}) {
	ctxlog.Warnf(ctx, log.AddCallerSkip(Log, 1), format, args...)
}

func WarnfWithLogger(ctx context.Context, l log.Logger, format string, args ...interface{}) {
	ctxlog.Warnf(ctx, log.AddCallerSkip(l, 1), format, args...)
}

func Errorf(ctx context.Context, format string, args ...interface{}) {
	ctxlog.Errorf(ctx, log.AddCallerSkip(Log, 1), format, args...)
}

func ErrorfWithLogger(ctx context.Context, l log.Logger, format string, args ...interface{}) {
	ctxlog.Errorf(ctx, log.AddCallerSkip(l, 1), format, args...)
}

func Fatalf(ctx context.Context, format string, args ...interface{}) {
	ctxlog.Fatalf(ctx, log.AddCallerSkip(Log, 1), format, args...)
}

func FatalfWithLogger(ctx context.Context, l log.Logger, format string, args ...interface{}) {
	ctxlog.Fatalf(ctx, log.AddCallerSkip(l, 1), format, args...)
}
