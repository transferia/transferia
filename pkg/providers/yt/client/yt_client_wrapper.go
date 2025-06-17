//go:build !disable_yt_provider

package ytclient

import (
	"fmt"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"go.ytsaurus.tech/yt/go/yt/ytrpc"
)

type ClientType string

const (
	HTTP ClientType = "http"
	RPC  ClientType = "rpc"
)

type verboseErrLogger struct {
	internalLogger log.Logger
}

func newVerboseErrLogger(logger log.Logger) *verboseErrLogger {
	return &verboseErrLogger{
		internalLogger: logger,
	}
}

func extractVerboseError(err error) (value string, ok bool) {
	switch err.(type) {
	case fmt.Formatter:
		// note: applicable to errors verbosity is configured in xerrors standard library:
		// https://github.com/transferia/transferia/arcadia/vendor/golang.org/x/xerrors/adaptor.go?rev=r4433436#L46
		errorVerbose := fmt.Sprintf("%+v", err)
		if errorVerbose != err.Error() {
			return errorVerbose, true
		}
	default:
	}
	return "", false
}

// replaceErrorFieldWithVerboseError
// This method iterates through error fields which you may usually see in logs like log.Error(err)
// The zap usually puts `err.Error()` as "error" field, but if error is rich formattable then it puts this result
// into "errorVerbose" field if it differs from content of the "error" field.
// Reference how library zap makes this: https://github.com/transferia/transferia/arcadia/vendor/go.uber.org/zap/zapcore/error.go?rev=r12909533#L73
// Note: rich error is such error that implements fmt.Formatter and can be formatted as fmt.Sprintf("%+v", basicErr)
//
// So, as we do not show user errorVerbose, but for YT logger we want to show errorVerbose format, we reformat rich error
// by ourselves before zap takes charge and put result preventively into "error" field.
func (y *verboseErrLogger) replaceErrorFieldWithVerboseError(fields []log.Field) []log.Field {
	fieldsNew := make([]log.Field, 0, len(fields))
	for _, field := range fields {
		fieldToAdd := field
		if field.Type() == log.FieldTypeError {
			errorVerbose, ok := extractVerboseError(field.Error())
			if ok {
				// override "error" structure log with "errorVerbose" value
				// note, that zap will not make "errorVerbose" field anymore because it is equal to "error" field now.
				fieldToAdd = log.Error(xerrors.New(errorVerbose))
			}
		}
		fieldsNew = append(fieldsNew, fieldToAdd)
	}
	return fieldsNew
}

func (y *verboseErrLogger) Trace(msg string, fields ...log.Field) {
	newFields := y.replaceErrorFieldWithVerboseError(fields)
	y.internalLogger.Trace(msg, newFields...)
}

func (y *verboseErrLogger) Debug(msg string, fields ...log.Field) {
	newFields := y.replaceErrorFieldWithVerboseError(fields)
	y.internalLogger.Debug(msg, newFields...)
}

func (y *verboseErrLogger) Info(msg string, fields ...log.Field) {
	newFields := y.replaceErrorFieldWithVerboseError(fields)
	y.internalLogger.Info(msg, newFields...)
}

func (y *verboseErrLogger) Warn(msg string, fields ...log.Field) {
	newFields := y.replaceErrorFieldWithVerboseError(fields)
	y.internalLogger.Warn(msg, newFields...)
}

func (y *verboseErrLogger) Error(msg string, fields ...log.Field) {
	newFields := y.replaceErrorFieldWithVerboseError(fields)
	y.internalLogger.Error(msg, newFields...)
}

func (y *verboseErrLogger) Fatal(msg string, fields ...log.Field) {
	newFields := y.replaceErrorFieldWithVerboseError(fields)
	y.internalLogger.Fatal(msg, newFields...)
}

func (y *verboseErrLogger) Tracef(format string, args ...interface{}) {
	y.internalLogger.Tracef(format, args...)
}

func (y *verboseErrLogger) Debugf(format string, args ...interface{}) {
	y.internalLogger.Debugf(format, args...)
}

func (y *verboseErrLogger) Infof(format string, args ...interface{}) {
	y.internalLogger.Infof(format, args...)
}

func (y *verboseErrLogger) Warnf(format string, args ...interface{}) {
	y.internalLogger.Warnf(format, args...)
}

func (y *verboseErrLogger) Errorf(format string, args ...interface{}) {
	y.internalLogger.Errorf(format, args...)
}

func (y *verboseErrLogger) Fatalf(format string, args ...interface{}) {
	y.internalLogger.Fatalf(format, args...)
}

func (y *verboseErrLogger) WithName(name string) log.Logger {
	yCopy := *y
	yCopy.internalLogger = y.internalLogger.WithName(name)
	return &yCopy
}

func (y *verboseErrLogger) Structured() log.Structured {
	return y
}

func (y *verboseErrLogger) Fmt() log.Fmt {
	return y
}

func (y *verboseErrLogger) Logger() log.Logger {
	return y
}

// NewYtClientWrapper creates YT client and make operations to extract correct logger for YT
// from second parameter which user passes. You may also can pass any logger or none at all (nil), then
// it should work properly as you create client with NewClient function of YT library by yourself.
//
// Usage example with four commonly used parameters around Data Transfer code:
//
//	client, err := ytclient.NewYtClientWrapper(ytclient.HTTP, logger, &yt.Config{
//		Proxy:                 dst.Cluster(),
//		Token:                 dst.Token(),
//		AllowRequestsFromJob:  true,
//		DisableProxyDiscovery: dst.GetConnectionData().DisableProxyDiscovery,
//	})
//	if err != nil {
//		return nil, xerrors.Errorf("unable to initialize yt client: %w", err)
//	}
func NewYtClientWrapper(clientType ClientType, lgr log.Logger, ytConfig *yt.Config) (yt.Client, error) {
	if ytConfig != nil {
		if lgr != nil {
			if ytConfig.Logger == nil {
				ytLgr := logger.ExtractYTLogger(lgr)
				ytConfig.Logger = newVerboseErrLogger(ytLgr).Structured()
			} else {
				return nil, xerrors.Errorf("program error: logger specified both in configuration and in parameter of YT client wrapper constructor, developer should choose only one option")
			}
		}
	}
	switch clientType {
	case RPC:
		ytRPCClient, err := ytrpc.NewClient(ytConfig)
		if err != nil {
			return nil, xerrors.Errorf("cannot create YT RPC client: %w", err)
		}
		return ytRPCClient, nil
	case HTTP:
		ytHTTPClient, err := ythttp.NewClient(ytConfig)
		if err != nil {
			return nil, xerrors.Errorf("cannot create YT HTTP client: %w", err)
		}
		return ytHTTPClient, nil
	default:
		return nil, xerrors.Errorf("unknown type of YT client: %s", clientType)
	}
}
