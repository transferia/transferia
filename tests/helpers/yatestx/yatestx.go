package yatestx

import (
	"path"

	"github.com/transferia/transferia/library/go/test/yatest"
)

func ProjectSource(src string) string {
	if path.IsAbs(src) {
		return src
	}
	project := yatest.ProjectPath()
	if project == "" {
		// Plain go test runs in the package directory.
		return src
	}
	return yatest.SourcePath(project + "/" + src)
}
