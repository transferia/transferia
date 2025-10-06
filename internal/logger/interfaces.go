package logger

import "io"

type Writer interface {
	io.Writer

	CanWrite() bool
}
