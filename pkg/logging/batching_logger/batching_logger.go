package batching_logger

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/transferia/transferia/pkg/util/size"
)

const maxStringLengthConst = size.MiB / 2

type loggingFunc func(in string)
type BatchingLogger struct {
	// config
	loggingFunc     loggingFunc
	header          string
	delimiter       string
	addTime         bool
	maxStringLength int
	// state
	state      []string
	sumLen     int
	flushCount int
}

func (l *BatchingLogger) flush(isFromClose bool) {
	lastStr := ""
	if isFromClose {
		if len(l.state) == 0 {
			return // is last flush has empty list (nothing was logged) - skip it to not to log just header
		}
		lastStr = ":LAST"
	}
	sumString := fmt.Sprintf("[batching-logger] [flush#%d%s] ", l.flushCount, lastStr) + l.header + strings.Join(l.state, l.delimiter)
	l.flushLine(sumString)
}
func (l *BatchingLogger) flushLine(in string) {
	l.loggingFunc(in)
	l.state = make([]string, 0)
	l.sumLen = 0
	l.flushCount++
}
func (l *BatchingLogger) splitAndLog(in string) {
	chunksIter := slices.Chunk([]rune(in), l.maxStringLength)
	chunks := slices.Collect(chunksIter)
	for i, chunk := range chunks {
		sumString := fmt.Sprintf("[batching-logger] [flush#%d] [PARTS:%d/%d] ", l.flushCount, i, len(chunks)) + l.header + string(chunk)
		l.flushLine(sumString)
	}
}
func (l *BatchingLogger) Log(in string) {
	if l.sumLen+len(in) > l.maxStringLength {
		l.flush(false)
		if len(in) > l.maxStringLength {
			l.splitAndLog(in)
			return
		}
	}
	prefix := ""
	if l.addTime {
		prefix = "[" + time.Now().Format("2006-01-02T15:04:05.000000000") + "]:"
	}
	l.state = append(l.state, prefix+in)
	l.sumLen += len(in)
}
func (l *BatchingLogger) Close() {
	l.flush(true)
}
func NewBatchingLogger(inLoggingFunc loggingFunc, header string, delimiter string, addTime bool) *BatchingLogger {
	return &BatchingLogger{
		loggingFunc:     inLoggingFunc,
		header:          header,
		delimiter:       delimiter,
		addTime:         addTime,
		maxStringLength: maxStringLengthConst,
		state:           make([]string, 0),
		sumLen:          0,
		flushCount:      0,
	}
}
