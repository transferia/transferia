//go:build !disable_s3_provider

package sink

import (
	"bytes"
	"compress/gzip"
	"io"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

type SnapshotGzip struct {
	feedChannel chan []byte
	writer      *gzip.Writer
	buffer      *bytes.Buffer
	closed      bool
	remainder   []byte
}

func (b *SnapshotGzip) Read(buf []byte) (n int, err error) {
	for {
		deltaN := copy(buf, b.remainder)
		n += deltaN
		b.remainder = b.remainder[deltaN:]
		if n > 0 {
			return n, nil
		}
		if b.closed {
			return 0, io.EOF
		}
		// remainder exhausted, read the next chunk from the feed channel
		data, ok := <-b.feedChannel
		if !ok {
			if err := b.writer.Flush(); err != nil {
				return 0, xerrors.Errorf("unable to flush data: %w", err)
			}
			if err := b.writer.Close(); err != nil {
				return 0, xerrors.Errorf("unable to close gzipper: %w", err)
			}
			// after writer close we have gzip footer, so add it to remainder and close reader
			b.remainder = b.buffer.Bytes()
			b.closed = true
			continue
		}
		if _, err := b.writer.Write(data); err != nil {
			return 0, xerrors.Errorf("unable to zip part: %w", err)
		}
		if err := b.writer.Flush(); err != nil {
			return 0, xerrors.Errorf("unable to flush data: %w", err)
		}
		b.remainder = b.buffer.Bytes()
		b.buffer.Reset()
	}
}

func (b *SnapshotGzip) FeedChannel() chan<- []byte {
	return b.feedChannel
}

func (b *SnapshotGzip) Close() {
	close(b.feedChannel)
}

func NewSnapshotGzip() *SnapshotGzip {
	var bb bytes.Buffer
	w := gzip.NewWriter(&bb)
	reader := &SnapshotGzip{
		feedChannel: make(chan []byte),
		writer:      w,
		buffer:      &bb,
		closed:      false,
		remainder:   nil,
	}
	return reader
}
