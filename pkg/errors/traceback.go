package errors

import (
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

func parseQualifiedName(qualifiedName string) (pkg, typ, method string) {
	parts := strings.Split(qualifiedName, "/")
	if len(parts) == 0 {
		return "", "", ""
	}
	lastPart := parts[len(parts)-1]
	components := strings.Split(lastPart, ".")

	if len(components) < 2 {
		return "", "", ""
	}

	switch len(components) {
	// no receiver, so typ is empty string
	case 2:
		return components[0], "", components[1]
	case 3:
		return components[0], components[1], components[2]
	// parsing func path becomes tricky if package name contains "."
	// as best effort - we assume last two components always type and method
	default:
		pkg = strings.Join(components[:len(components)-2], ".")
		typ = components[len(components)-2]
		method = components[len(components)-1]
		return pkg, typ, method
	}
}

func ExtractShortStackTrace(err error) string {
	return strings.ReplaceAll(extractStackTrace(err), ".CategorizedErrorf", "")
}

func extractStackTrace(err error) string {
	if err == nil {
		return ""
	}
	errStack, traceAble := err.(xerrors.ErrorStackTrace)
	var currMsg string
	if traceAble {
		frames := errStack.StackTrace().Frames()
		b := strings.Builder{}
		// frames always consist of a single frame
		frame := frames[0]
		_, typ, method := parseQualifiedName(frame.Function)
		if typ != "" {
			b.WriteString(typ + ".")
		}
		b.WriteString(method)
		currMsg = b.String()
	}
	unWrapper, unWrapable := err.(interface{ Unwrap() error })
	if !unWrapable && !traceAble {
		return currMsg
	}
	if unWrapable {
		err = unWrapper.Unwrap()
	}

	prev := extractStackTrace(err)
	if prev == "" {
		return currMsg
	}
	if currMsg == "" {
		return prev
	}
	return strings.Join([]string{prev, currMsg}, ".")
}
