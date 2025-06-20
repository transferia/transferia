//go:build !disable_s3_provider

package sink

import (
	"io"
)

type SnapshotRaw struct {
	feedChannel chan []byte
	remainder   []byte
}

func (b *SnapshotRaw) Read(buf []byte) (n int, err error) {
	for {
		deltaN := copy(buf, b.remainder)
		n += deltaN
		b.remainder = b.remainder[deltaN:]
		buf = buf[deltaN:]
		if len(buf) == 0 {
			// the output buffer is full, return the number of bytes copied so far
			return n, nil
		}

		// remainder exhausted, read the next chunk from the feed channel
		var ok bool
		b.remainder, ok = <-b.feedChannel
		if !ok {
			return n, io.EOF
		}
	}
}

func (b *SnapshotRaw) FeedChannel() chan<- []byte {
	return b.feedChannel
}

func (b *SnapshotRaw) Close() {
	close(b.feedChannel)
}

func NewSnapshotRaw() *SnapshotRaw {
	return &SnapshotRaw{
		feedChannel: make(chan []byte),
		remainder:   nil,
	}
}
