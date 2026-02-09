package batching_logger

import (
	"strings"

	"go.ytsaurus.tech/library/go/core/log"
)

type BatchingLoggerable interface {
	ToBatchingLines() []string
}

func fieldsToString(fields ...log.Field) string {
	fieldsArr := make([]string, 0)
	for _, field := range fields {
		fieldsArr = append(fieldsArr, field.String())
	}
	return strings.Join(fieldsArr, " ")
}

func LogLine(throttler Throttler, inLoggingFunc loggingFunc, msg string, fields ...log.Field) {
	if !throttler.IsAllowed() {
		inLoggingFunc("[batching-logger] skipped 'LogLine' by throttler")
		return
	}
	throttler.WillLog()

	currLogger := NewBatchingLogger(NewAbsentThrottler(), inLoggingFunc, "", "", false)
	defer currLogger.Close()
	sumStr := msg + fieldsToString(fields...)
	currLogger.Log(sumStr)
}

func LogLines(inLogger log.Logger, header string, inLines BatchingLoggerable) {
	currLogger := NewBatchingLogger(
		NewAbsentThrottler(),
		func(in string) { inLogger.Info(in) },
		header,
		",",
		false,
	)
	defer currLogger.Close()
	lines := inLines.ToBatchingLines()
	for _, currLine := range lines {
		currLogger.Log(currLine)
	}
}
