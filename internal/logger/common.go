package logger

import (
	"os"

	"github.com/transferia/transferia/pkg/instanceutil"
	zp "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.ytsaurus.tech/library/go/core/log"
	corezap "go.ytsaurus.tech/library/go/core/log/zap"
)

var FatalErrorLog log.Logger = &corezap.Logger{L: zp.NewNop()}

type LogLevelSetter struct {
	LogLevel string `yaml:"log_level"`
}

func LogLevel() string {
	if os.Getenv("LOG_LEVEL") != "" {
		return os.Getenv("LOG_LEVEL")
	}
	userData := new(LogLevelSetter)
	if err := instanceutil.GetGoogleCEUserData(userData); err == nil && userData.LogLevel != "" {
		return userData.LogLevel
	}
	return "INFO"
}

func NewConsoleLogger() log.Logger {
	consoleLevel := getEnvLogLevels()
	defaultPriority := levelEnablerFactory(consoleLevel.Zap)
	syncStderr := zapcore.AddSync(os.Stderr)
	stdErrEncoder := zapcore.NewConsoleEncoder(corezap.CLIConfig(consoleLevel.Log).EncoderConfig)
	lbCore := zapcore.NewTee(
		zapcore.NewCore(stdErrEncoder, syncStderr, defaultPriority),
	)

	return newLogger(lbCore)
}

func newLogger(core zapcore.Core) log.Logger {
	return &corezap.Logger{
		L: zp.New(
			core,
			zp.AddCaller(),
			zp.AddCallerSkip(1),
			zp.AddStacktrace(zp.WarnLevel),
		),
	}
}

func levelEnablerFactory(zapLvl zapcore.Level) zapcore.LevelEnabler {
	return zp.LevelEnablerFunc(func(l zapcore.Level) bool {
		return l >= zapLvl
	})
}

func copySlice(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
