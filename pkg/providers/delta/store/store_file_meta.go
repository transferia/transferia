//go:build !disable_delta_provider

package store

import (
	"time"
)

type FileMeta struct {
	path         string
	timeModified time.Time
	size         uint64
}

func (f *FileMeta) Path() string {
	return f.path
}

func (f *FileMeta) TimeModified() time.Time {
	return f.timeModified
}

func (f *FileMeta) Size() uint64 {
	return f.size
}
