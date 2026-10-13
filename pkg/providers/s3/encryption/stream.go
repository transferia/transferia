package encryption

import (
	"io"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

var _ io.WriteCloser = (*encryptingWriter)(nil)

// encryptingWriter encrypts everything written to it into dst.
type encryptingWriter struct {
	dst io.WriteCloser
	key *Key
	// stream is created on the first Write or Close: creating it writes the header to dst, and S3 sinks create
	// writers over an io.Pipe before they start the upload that reads from it.
	stream io.WriteCloser
	closed bool
}

// NewEncryptingWriter returns a writer that encrypts data into dst. Close writes the last segment and closes dst.
// Every writer uses its own random salt, so encrypting the same data twice gives different ciphertexts.
func NewEncryptingWriter(dst io.WriteCloser, key *Key) io.WriteCloser {
	return &encryptingWriter{
		dst:    dst,
		key:    key,
		stream: nil,
		closed: false,
	}
}

func (w *encryptingWriter) Write(p []byte) (int, error) {
	if w.closed {
		// The same error as a write to a closed pipe: S3 sinks check for it when they close writers twice.
		return 0, io.ErrClosedPipe
	}
	if err := w.startStream(); err != nil {
		return 0, xerrors.Errorf("unable to start encryption: %w", err)
	}
	return w.stream.Write(p)
}

// Close writes the last segment and closes dst. An empty input still produces a valid ciphertext: the header and
// one empty authenticated segment. Repeated calls do nothing.
func (w *encryptingWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if err := w.finishStream(); err != nil {
		// A plain Close of an S3 sink pipe would complete the upload of a truncated object, so the upload is failed.
		closeWithError(w.dst, err)
		return xerrors.Errorf("unable to finish encryption: %w", err)
	}
	if err := w.dst.Close(); err != nil {
		return xerrors.Errorf("unable to close encrypted stream: %w", err)
	}
	return nil
}

func (w *encryptingWriter) finishStream() error {
	if err := w.startStream(); err != nil {
		return xerrors.Errorf("unable to start encryption: %w", err)
	}
	if err := w.stream.Close(); err != nil {
		return xerrors.Errorf("unable to write the last encrypted segment: %w", err)
	}
	return nil
}

func closeWithError(dst io.Closer, err error) {
	if pipe, ok := dst.(interface{ CloseWithError(error) error }); ok {
		_ = pipe.CloseWithError(err)
		return
	}
	_ = dst.Close()
}

func (w *encryptingWriter) startStream() error {
	if w.stream != nil {
		return nil
	}
	streamingAEAD, err := w.key.streamingAEAD()
	if err != nil {
		return xerrors.Errorf("unable to create streaming AEAD: %w", err)
	}
	stream, err := streamingAEAD.NewEncryptingWriter(w.dst, associatedData)
	if err != nil {
		return xerrors.Errorf("unable to write encryption header: %w", err)
	}
	w.stream = stream
	return nil
}

// NewDecryptingReader returns a reader that decrypts src sequentially. It reads the header from src immediately.
// Read returns io.EOF only after the final segment is authenticated, so a corrupted, reordered, truncated or extended
// ciphertext fails with ErrAuthentication instead of producing partial data.
func NewDecryptingReader(src io.Reader, key *Key) (io.Reader, error) {
	header := make([]byte, headerSize)
	n, _, err := readFull(src, header)
	if err != nil {
		return nil, xerrors.Errorf("unable to read encryption header: %w", err)
	}
	if n < headerSize {
		return nil, ErrAuthentication.Wrap(xerrors.New("the object is shorter than the encryption header"))
	}
	segmentCipher, err := newSegmentCipher(header, key)
	if err != nil {
		return nil, xerrors.Errorf("unable to read encryption header: %w", err)
	}
	return &decryptingReader{
		src:             src,
		cipher:          segmentCipher,
		index:           0,
		buffer:          make([]byte, ciphertextSegmentSize+1),
		pending:         0,
		plaintextBuffer: make([]byte, 0, ciphertextSegmentSize),
		plaintext:       nil,
		done:            false,
	}, nil
}

var _ io.Reader = (*decryptingReader)(nil)

// decryptingReader decrypts segments one by one. Tink-go has a sequential reader, but it treats a ciphertext truncated
// right after a non-final segment and one more byte as a complete one, so the reader of the format is ours, like ReaderAt.
type decryptingReader struct {
	src    io.Reader
	cipher *segmentCipher
	index  int64
	// buffer holds a segment and one byte more: a segment is the final one exactly when nothing follows it.
	buffer []byte
	// pending is the number of bytes of the next segment already read into the beginning of buffer.
	pending int
	// plaintextBuffer is reused for every segment: a segment is decrypted only after the previous one is returned.
	plaintextBuffer []byte
	// plaintext is the decrypted part of the current segment that is not returned yet.
	plaintext []byte
	// done is set when the final segment is authenticated.
	done bool
}

func (r *decryptingReader) Read(p []byte) (int, error) {
	for len(r.plaintext) == 0 {
		if r.done {
			return 0, io.EOF
		}
		if err := r.decryptNextSegment(); err != nil {
			return 0, err
		}
	}
	n := copy(p, r.plaintext)
	r.plaintext = r.plaintext[n:]
	return n, nil
}

func (r *decryptingReader) decryptNextSegment() error {
	segmentSize := ciphertextSegmentSize
	if r.index == 0 {
		segmentSize -= headerSize
	}
	n, last, err := readFull(r.src, r.buffer[r.pending:segmentSize+1])
	if err != nil {
		return xerrors.Errorf("unable to read encrypted segment %d: %w", r.index+1, err)
	}
	if last {
		segmentSize = r.pending + n
	}

	ciphertext := r.buffer[:segmentSize]
	// The plaintext is decrypted into a separate buffer: the byte after the segment stays in r.buffer.
	plaintext, err := r.cipher.open(r.plaintextBuffer[:0], ciphertext, r.index, last)
	if err != nil {
		return xerrors.Errorf("unable to decrypt: %w", err)
	}
	if !last {
		r.buffer[0] = r.buffer[segmentSize]
		r.pending = 1
	}
	r.plaintext = plaintext
	r.index++
	r.done = last
	return nil
}

// readFull fills p like io.ReadFull and reports whether src ended before p was filled. Only io.EOF of src ends the
// ciphertext: an HTTP body cut short returns io.ErrUnexpectedEOF, which stays a read failure to retry rather than
// an authentication failure of a seemingly truncated object.
func readFull(src io.Reader, p []byte) (int, bool, error) {
	n := 0
	for n < len(p) {
		m, err := src.Read(p[n:])
		n += m
		if n == len(p) {
			return n, false, nil
		}
		if xerrors.Is(err, io.EOF) {
			return n, true, nil
		}
		if err != nil {
			return n, false, err
		}
	}
	return n, false, nil
}
