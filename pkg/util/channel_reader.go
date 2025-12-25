package util

import (
	"io"

	mathutil "github.com/transferia/transferia/pkg/util/math"
)

type ChannelReader struct {
	inputCh chan []byte
	buffer  []byte
	eof     bool
}

func (r *ChannelReader) Close() {
	close(r.inputCh)
}

func (r *ChannelReader) Input() chan<- []byte {
	return r.inputCh
}

func (r *ChannelReader) Read(dst []byte) (int, error) {
	if r.eof {
		return 0, io.EOF
	}

	if len(dst) == 0 {
		return 0, nil
	}

	for len(r.buffer) == 0 {
		var ok bool
		if r.buffer, ok = <-r.inputCh; !ok {
			r.eof = true
			return 0, io.EOF
		}
	}

	n := mathutil.Min(len(dst), len(r.buffer))
	copy(dst, r.buffer[:n])
	r.buffer = r.buffer[n:]
	return n, nil
}

func NewChannelReader() *ChannelReader {
	return &ChannelReader{
		inputCh: make(chan []byte),
		buffer:  nil,
		eof:     false,
	}
}
