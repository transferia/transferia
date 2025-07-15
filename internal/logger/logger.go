package logger

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/mattn/go-isatty"
	_ "go.opentelemetry.io/contrib/bridges/otelzap"
	zp "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/go/mapreduce"
)

// Дефолтный логгер.
// В ыте будет консольный.
// В nanny будет json-ый.
// В дев тачке будет прекрасный.
var (
	Log     log.Logger
	NullLog *zap.Logger
)

type Factory func(context.Context) log.Logger

func DummyLoggerFactory(ctx context.Context) log.Logger {
	return Log
}

func LoggerWithLevel(lvl zapcore.Level) log.Logger {
	cfg := zp.Config{
		Level:            zp.NewAtomicLevelAt(lvl),
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
	return log.With(zap.Must(cfg)).(*zap.Logger)
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

func DefaultLoggerConfig(level zapcore.Level) zp.Config {
	encoder := zapcore.CapitalColorLevelEncoder
	if !isatty.IsTerminal(os.Stdout.Fd()) || !isatty.IsTerminal(os.Stderr.Fd()) {
		encoder = zapcore.CapitalLevelEncoder
	}

	return zp.Config{
		Level:            zp.NewAtomicLevelAt(level),
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
		cfg = zap.JSONConfig(level.Log)
	}

	if os.Getenv("CI") == "1" || strings.Contains(os.Args[0], "gotest") {
		cfg = zp.Config{
			Level:            zp.NewAtomicLevelAt(zp.DebugLevel),
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
	if mapreduce.InsideJob() {
		cfg = zp.Config{
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
	ytCfg.Level = zp.NewAtomicLevelAt(ytLogLevel.Zap)

	host, _ := os.Hostname()
	logger := zap.Must(cfg)
	ytLogger := zap.Must(ytCfg)
	Log = log.With(NewYtLogBundle(logger, ytLogger), log.Any("host", host)).(YtLogBundle)
}
