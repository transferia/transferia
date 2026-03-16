package yatestx

import (
	"path"

	"github.com/transferia/transferia/library/go/test/yatest"
)

func ProjectSource(src string) string {
	if path.IsAbs(src) {
		return src
	}
	return yatest.SourcePath(yatest.ProjectPath() + "/" + src)
}
