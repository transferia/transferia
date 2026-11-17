package yatestx

import (
	"path"

	"github.com/transferia/transferia/library/go/test/yatest"
)

func ProjectSource(src string) string {
	if path.IsAbs(src) {
		return src
	}
	// Plain go test, in Arcadia or in the GitHub export, runs in the package directory.
	if !yatest.HasYaTestContext() || yatest.ProjectPath() == "" {
		return src
	}
	return yatest.SourcePath(yatest.ProjectPath() + "/" + src)
}
