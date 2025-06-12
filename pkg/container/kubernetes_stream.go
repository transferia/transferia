package container

import "io"

// streamWrapper signals when the stream is closed
type streamWrapper struct {
	io.ReadCloser
	onClose func()
	closed  bool
}

func (s *streamWrapper) Close() error {
	if !s.closed {
		s.closed = true
		defer s.onClose()
	}
	return s.ReadCloser.Close()
}
