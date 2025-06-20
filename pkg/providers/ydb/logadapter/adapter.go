//go:build !disable_ydb_provider

package logadapter

import (
	"context"

	ydbLog "github.com/ydb-platform/ydb-go-sdk/v3/log"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ ydbLog.Logger = *new(adapter)

type adapter struct {
	l log.Logger
}

func (a adapter) Log(ctx context.Context, msg string, fields ...ydbLog.Field) {
	l := a.l
	for _, name := range ydbLog.NamesFromContext(ctx) {
		l = l.WithName(name)
	}

	switch ydbLog.LevelFromContext(ctx) {
	case ydbLog.TRACE:
		l.Trace(msg, ToCoreFields(fields)...)
	case ydbLog.DEBUG:
		l.Debug(msg, ToCoreFields(fields)...)
	case ydbLog.INFO:
		l.Info(msg, ToCoreFields(fields)...)
	case ydbLog.WARN:
		l.Warn(msg, ToCoreFields(fields)...)
	case ydbLog.ERROR:
		l.Error(msg, ToCoreFields(fields)...)
	case ydbLog.FATAL:
		l.Fatal(msg, ToCoreFields(fields)...)
	default:
	}
}
