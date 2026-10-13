package s3raw

import (
	"io"
	"sync"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/s3/encryption"
)

var _ S3RawReader = (*decryptingReader)(nil)

// decryptingReader reads an object encrypted by an S3 destination with client-side encryption.
// Read decrypts the object stream sequentially. ReadAt decrypts only the segments it needs, so parquet readers fetch
// the footer and column chunks with range requests, as they do for plaintext objects.
type decryptingReader struct {
	ciphertext S3RawReader
	key        *encryption.Key

	// stream is the sequential plaintext; nil until the first Read and after Close, like in other readers.
	stream io.Reader

	// readerAt is created on the first successful ReadAt: it reads the header and authenticates the end of the
	// object. Failures are not cached, so a transient S3 error does not break later reads.
	readerAtMu sync.Mutex
	readerAt   *encryption.ReaderAt
}

func (r *decryptingReader) Read(p []byte) (int, error) {
	if r.stream == nil {
		stream, err := encryption.NewDecryptingReader(r.ciphertext, r.key)
		if err != nil {
			return 0, xerrors.Errorf("unable to start decryption: %w", err)
		}
		r.stream = stream
	}
	return r.stream.Read(p)
}

func (r *decryptingReader) ReadAt(p []byte, off int64) (int, error) {
	readerAt, err := r.openReaderAt()
	if err != nil {
		return 0, xerrors.Errorf("unable to open encrypted object: %w", err)
	}
	return readerAt.ReadAt(p, off)
}

func (r *decryptingReader) openReaderAt() (*encryption.ReaderAt, error) {
	r.readerAtMu.Lock()
	defer r.readerAtMu.Unlock()
	if r.readerAt == nil {
		readerAt, err := encryption.NewReaderAt(r.ciphertext, r.ciphertext.Size(), r.key)
		if err != nil {
			return nil, xerrors.Errorf("unable to read encryption header and last segment: %w", err)
		}
		r.readerAt = readerAt
	}
	return r.readerAt, nil
}

func (r *decryptingReader) Close() error {
	r.stream = nil
	return r.ciphertext.Close()
}

// Size returns the plaintext size. For a damaged object it returns 0: reads of such an object fail anyway.
func (r *decryptingReader) Size() int64 {
	size, err := encryption.PlaintextSize(r.ciphertext.Size())
	if err != nil {
		return 0
	}
	return size
}

func (r *decryptingReader) LastModified() time.Time {
	return r.ciphertext.LastModified()
}

func newDecryptingReader(ciphertext S3RawReader, key *encryption.Key) *decryptingReader {
	return &decryptingReader{
		ciphertext: ciphertext,
		key:        key,
		stream:     nil,
		readerAtMu: sync.Mutex{},
		readerAt:   nil,
	}
}
