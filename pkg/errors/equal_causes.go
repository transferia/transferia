package errors

import (
	"runtime"

	"github.com/transferria/transferria/library/go/core/xerrors"
)

// EqualCauses checks if two errors have the same cause. This is determined by an heuristic defined in this function.
func EqualCauses(a error, b error) bool {
	if a == nil || b == nil {
		return false
	}
	frameA, exists := extractCauseFrame(a)
	if !exists {
		return false
	}
	frameB, exists := extractCauseFrame(b)
	if !exists {
		return false
	}
	return frameA.Function == frameB.Function && frameA.Line == frameB.Line
}

func extractCauseFrame(e error) (result runtime.Frame, exists bool) {
	xStackTrace := xerrors.StackTraceOfCause(e)
	if xStackTrace == nil {
		return runtime.Frame{}, false
	}
	rStackTrace := xStackTrace.StackTrace()
	if rStackTrace == nil {
		return runtime.Frame{}, false
	}
	frames := rStackTrace.Frames()
	if len(frames) == 0 {
		return runtime.Frame{}, false
	}
	return frames[0], true
}
