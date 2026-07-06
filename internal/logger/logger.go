package logger

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/mattn/go-isatty"
	"github.com/transferia/transferia/tests/helpers/testsflag"
	_ "go.opentelemetry.io/contrib/bridges/otelzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.ytsaurus.tech/library/go/core/log"
	ya_zap "go.ytsaurus.tech/library/go/core/log/zap"
)

// Дефолтный логгер.
// В ыте будет консольный.
// В nanny будет json-ый.
// В дев тачке будет прекрасный.
var (
	Log     log.Logger
	NullLog *ya_zap.Logger
)

type Factory func(context.Context) log.Logger

func DummyLoggerFactory(ctx context.Context) log.Logger {
	return Log
}

func LoggerWithLevel(lvl zapcore.Level) log.Logger {
	cfg := zap.Config{
		Level:            zap.NewAtomicLevelAt(lvl),
		Encoding:         "console",
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:     "msg",
			LevelKey:       "level",
			TimeKey:        "ts",
			CallerKey:      "caller",
			EncodeLevel:    zapcore.CapitalColorLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   AdditionalComponentCallerEncoder,
		},
	}
	return log.With(ya_zap.Must(cfg)).(*ya_zap.Logger)
}

func AdditionalComponentCallerEncoder(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
	path := caller.String()
	lastIndex := len(path) - 1
	for i := 0; i < 3; i++ {
		lastIndex = strings.LastIndex(path[0:lastIndex], "/")
		if lastIndex == -1 {
			break
		}
	}
	if lastIndex > 0 {
		path = path[lastIndex+1:]
	}
	enc.AppendString(path)
}

type levels struct {
	Zap zapcore.Level
	Log log.Level
}

func getEnvLogLevels() levels {
	if level, ok := os.LookupEnv("LOG_LEVEL"); ok {
		return parseLevel(level)
	}
	return levels{zapcore.InfoLevel, log.InfoLevel}
}

func getEnvYtLogLevel() levels {
	if level, ok := os.LookupEnv("YT_LOG_LEVEL"); ok {
		return parseLevel(level)
	}
	return levels{zapcore.DebugLevel, log.DebugLevel}
}

func parseLevel(level string) levels {
	zpLvl := zapcore.InfoLevel
	lvl := log.InfoLevel
	if level != "" {
		fmt.Printf("overriden YT log level to: %v\n", level)
		var l zapcore.Level
		if err := l.UnmarshalText([]byte(level)); err == nil {
			zpLvl = l
		}
		var gl log.Level
		if err := gl.UnmarshalText([]byte(level)); err == nil {
			lvl = gl
		}
	}
	return levels{zpLvl, lvl}
}

func DefaultLoggerConfig(level zapcore.Level) zap.Config {
	encoder := zapcore.CapitalColorLevelEncoder
	if !isatty.IsTerminal(os.Stdout.Fd()) || !isatty.IsTerminal(os.Stderr.Fd()) {
		encoder = zapcore.CapitalLevelEncoder
	}

	return zap.Config{
		Level:            zap.NewAtomicLevelAt(level),
		Encoding:         "console",
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:     "msg",
			LevelKey:       "level",
			TimeKey:        "ts",
			CallerKey:      "caller",
			EncodeLevel:    encoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   AdditionalComponentCallerEncoder,
		},
	}
}

func init() {
	level := getEnvLogLevels()
	cfg := DefaultLoggerConfig(level.Zap)

	if os.Getenv("QLOUD_LOGGER_STDOUT_PARSER") == "json" {
		cfg = ya_zap.JSONConfig(level.Log)
	}

	if testsflag.IsTest() {
		cfg = zap.Config{
			Level:            zap.NewAtomicLevelAt(zap.DebugLevel),
			Encoding:         "console",
			OutputPaths:      []string{"stdout"},
			ErrorOutputPaths: []string{"stderr"},
			EncoderConfig: zapcore.EncoderConfig{
				MessageKey:     "msg",
				LevelKey:       "level",
				TimeKey:        "ts",
				CallerKey:      "caller",
				EncodeLevel:    zapcore.CapitalLevelEncoder,
				EncodeTime:     zapcore.ISO8601TimeEncoder,
				EncodeDuration: zapcore.StringDurationEncoder,
				EncodeCaller:   AdditionalComponentCallerEncoder,
			},
		}
	}
	if os.Getenv("YT_JOB_ID") != "" {
		cfg = zap.Config{
			Level:            cfg.Level,
			Encoding:         "console",
			OutputPaths:      []string{"stderr"},
			ErrorOutputPaths: []string{"stderr"},
			EncoderConfig: zapcore.EncoderConfig{
				MessageKey:     "msg",
				LevelKey:       "level",
				TimeKey:        "ts",
				CallerKey:      "caller",
				EncodeLevel:    zapcore.CapitalLevelEncoder,
				EncodeTime:     zapcore.ISO8601TimeEncoder,
				EncodeDuration: zapcore.StringDurationEncoder,
				EncodeCaller:   AdditionalComponentCallerEncoder,
			},
		}
	}

	ytCfg := cfg
	ytLogLevel := getEnvYtLogLevel()
	ytCfg.Level = zap.NewAtomicLevelAt(ytLogLevel.Zap)

	host, _ := os.Hostname()
	logger := ya_zap.Must(cfg)
	ytLogger := ya_zap.Must(ytCfg)
	Log = log.With(NewYtLogBundle(logger, ytLogger), log.Any("host", host)).(YtLogBundle)
}
