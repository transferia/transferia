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

	seen := make(map[error]bool)
	var result []string

	for err != nil {
		if seen[err] {
			break
		}
		seen[err] = true

		if errStack, ok := err.(xerrors.ErrorStackTrace); ok {
			frames := errStack.StackTrace().Frames()
			if len(frames) > 0 {
				frame := frames[0]
				_, typ, method := parseQualifiedName(frame.Function)
				var currMsg string
				if typ != "" {
					currMsg = typ + "." + method
				} else {
					currMsg = method
				}
				result = append([]string{currMsg}, result...)
			}
		}

		if unWrapper, ok := err.(interface{ Unwrap() error }); ok {
			err = unWrapper.Unwrap()
		} else {
			break
		}
	}

	return strings.Join(result, ".")
}
