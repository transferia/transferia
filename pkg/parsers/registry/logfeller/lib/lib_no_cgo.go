//go:build !cgo || !logfeller_parsers
// +build !cgo !logfeller_parsers

package lib

import (
	"time"

	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/parsers"
)

func SetConfigsStorage(useEmbeddedConfigs bool) {
	logger.Log.Warn("Parser is not supported on current OS")
}

func Parse(parser, splitter, transportMeta string, maskSecrets bool, msg parsers.Message) string {
	logger.Log.Warn("Parser is not supported on current OS")
	time.Sleep(time.Second * 1)
	return ""
}

func Schema(parser, splitter string) []abstract.ColSchema {
	return nil
}

func Resources(parser string) []string {
	logger.Log.Warn("Parser is not supported on current OS")
	return nil
}

func Enabled() bool {
	return false
}
