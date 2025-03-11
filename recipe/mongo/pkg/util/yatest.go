package util

import (
	"github.com/transferria/transferria/library/go/test/yatest"
)

func MakeOutputPath(relativePath string) string {
	if yatest.HasRAMDrive() {
		return yatest.OutputRAMDrivePath(relativePath)
	} else {
		return yatest.OutputPath(relativePath)
	}
}

func MakeWorkPath(relativePath string) string {
	if yatest.HasRAMDrive() {
		return yatest.RAMDrivePath(relativePath)
	} else {
		return yatest.WorkPath(relativePath)
	}
}
