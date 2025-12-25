package writers

import (
	"context"
	"io"
)

type Writer interface {
	io.Closer
	Write(ctx context.Context, data []byte) error
}
