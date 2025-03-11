package hostnameindex

import (
	"os"
	"strconv"
	"strings"

	"github.com/transferia/transferia/internal/logger"
	"go.ytsaurus.tech/library/go/core/log"
)

// Calculate returns the string containing a parsable integer - an index assigned to the current `os.Hostname()`.
//
// This method is useful when the `os.Hostname()` has a predefined structure.
func Calculate() string {
	hostname, err := os.Hostname()
	if err != nil {
		logger.Log.Error("os.Hostname() failed", log.Error(err))
		return "1"
	}
	tokens := strings.Split(hostname, "-")
	if len(tokens) < 2 {
		logger.Log.Error("os.Hostname() is not in expected format", log.String("hostname", hostname))
		return "1"
	}
	maybeIndex := tokens[len(tokens)-1]
	if _, err := strconv.Atoi(maybeIndex); err != nil {
		logger.Log.Error("os.Hostname() is not in expected format, contains non-parsable index", log.String("hostname", hostname))
		return "1"
	}
	return maybeIndex
}
