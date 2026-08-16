package logger

import (
	"go.ytsaurus.tech/library/go/core/log"
)

// YtLogBundle is a logger that holds reference to YT Logger in order to hook all "with's" modification and apply
// them syncrhonously. You may override other hooks.
type YtLogBundle interface {
	log.Logger
	// ExtractYTLogger extracts YT logger with different settings but all registered "with" values applied to main worker
	ExtractYTLogger() log.Logger
}

// LogBundle is a logger that holds reference to YT Logger in order to hook all "with's" modification and apply
// them syncrhonously. You may override other hooks.
type ytLogBundleImpl struct {
	log.Logger
	ytLogger log.Logger
}

// NewYtLogBundle constructs LogBundle from two loggers: original one and separate logger for YT. Acts like
// standard zap.Must(config), but hooks system log calls "With" to apply to zap.Must(ytConfig)
func NewYtLogBundle(log, ytLogger log.Logger) YtLogBundle {
	return &ytLogBundleImpl{
		Logger:   log,
		ytLogger: ytLogger,
	}
}

func (l *ytLogBundleImpl) With(fields ...log.Field) log.Logger {
	if l == nil {
		return nil
	}
	lCopy := *l
	lCopy.Logger = log.With(l.Logger, fields...)
	lCopy.ytLogger = log.With(l.ytLogger, fields...)
	return &lCopy
}

func (l *ytLogBundleImpl) AddCallerSkip(skip int) log.Logger {
	if l == nil {
		return nil
	}
	lCopy := *l
	lCopy.Logger = log.AddCallerSkip(l.Logger, skip)
	lCopy.ytLogger = log.AddCallerSkip(l.ytLogger, skip)
	return &lCopy
}

// ExtractYTLogger extracts preconfigured YT logger with corresponding registered 'With' calls
func (l *ytLogBundleImpl) ExtractYTLogger() log.Logger {
	return l.ytLogger
}

// ExtractYTLogger is a helper function to extract YT log. If the `lgr` parameter is not of thge type
// logger.YtLogBundle, then the log returned itself, otherwise log for YT is returned
func ExtractYTLogger(lgr log.Logger) log.Logger {
	if ytLogBundle, ok := lgr.(YtLogBundle); ok {
		lgr.Infof("YT Logger extracted successfully")
		return ytLogBundle.ExtractYTLogger()
	}
	lgr.Info("YT Logger wasn't extracted, use default logger",
		log.Sprintf("logger-type", "%T", lgr),
		log.Any("logger", lgr))
	return lgr
}
