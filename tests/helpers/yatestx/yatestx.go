package yatestx

import "github.com/transferia/transferia/library/go/test/yatest"

func ProjectSource(path string) string {
	return yatest.SourcePath(yatest.ProjectPath() + "/" + path)
}
