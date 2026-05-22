package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.ytsaurus.tech/library/go/core/log"
	ya_zap "go.ytsaurus.tech/library/go/core/log/zap"
)

var FatalErrorLog log.Logger = &ya_zap.Logger{L: zap.NewNop()}

func NewConsoleLogger() log.Logger {
	consoleLevel := getEnvLogLevels()
	defaultPriority := levelEnablerFactory(consoleLevel.Zap)
	syncStderr := zapcore.AddSync(os.Stderr)
	stdErrEncoder := zapcore.NewConsoleEncoder(ya_zap.CLIConfig(consoleLevel.Log).EncoderConfig)
	lbCore := zapcore.NewTee(
		zapcore.NewCore(stdErrEncoder, syncStderr, defaultPriority),
	)

	return newLogger(lbCore)
}

func newLogger(core zapcore.Core) log.Logger {
	return &ya_zap.Logger{
		L: zap.New(
			core,
			zap.AddCaller(),
			zap.AddCallerSkip(1),
			zap.AddStacktrace(zap.WarnLevel),
		),
	}
}

func levelEnablerFactory(zapLvl zapcore.Level) zapcore.LevelEnabler {
	return zap.LevelEnablerFunc(func(l zapcore.Level) bool {
		return l >= zapLvl
	})
}

func copySlice(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
