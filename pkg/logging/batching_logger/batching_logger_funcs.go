package batching_logger

import (
	"strings"

	"go.ytsaurus.tech/library/go/core/log"
)

type BatchingLoggerable interface {
	ToBatchingLines() []string
}

func LogLineInfo(inLoggingFunc loggingFunc, msg string, fields ...log.Field) {
	currLogger := NewBatchingLogger(inLoggingFunc, "", "", false)
	defer currLogger.Close()
	fieldsArr := make([]string, 0)
	for _, field := range fields {
		fieldsArr = append(fieldsArr, field.String())
	}
	sumStr := msg + strings.Join(fieldsArr, " ")
	currLogger.Log(sumStr)
}
func LogFileList(inLogger log.Logger, header string, inFiles BatchingLoggerable) {
	currLogger := NewBatchingLogger(
		func(in string) { inLogger.Info(in) },
		header,
		",",
		false,
	)
	defer currLogger.Close()
	inFilesLines := inFiles.ToBatchingLines()
	for _, currLine := range inFilesLines {
		currLogger.Log(currLine)
	}
}
