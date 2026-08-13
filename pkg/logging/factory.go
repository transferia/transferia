package logging

import (
	"context"
	"os"

	"github.com/transferia/transferia/pkg/transferparams"
	"go.ytsaurus.tech/library/go/core/log"
)

type LoggerFactory func(ctx context.Context) log.Logger

const DefaultLogLevel = "INFO"

func LogLevel() string {
	if os.Getenv("LOG_LEVEL") != "" {
		return os.Getenv("LOG_LEVEL")
	}
	transferParamsProvider, err := transferparams.GetProvider()
	if err != nil {
		return DefaultLogLevel
	}
	logLevel, err := transferParamsProvider.LogLevel()
	if err != nil || logLevel == "" {
		return DefaultLogLevel
	}
	return logLevel
}
