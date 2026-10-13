package encryption

import (
	"bytes"
	"io"
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

var _ io.ReaderAt = (*ReaderAt)(nil)

// ReaderAt decrypts arbitrary plaintext ranges of an encrypted object. Parquet readers need it to read the footer
// and column chunks without downloading the whole object. Tink-go does not provide seekable decryption
// (Java and C++ Tink do), so the segment arithmetic of the format lives here.
type ReaderAt struct {
	src            io.ReaderAt
	ciphertextSize int64
	plaintextSize  int64
	segmentCount   int64
	cipher         *segmentCipher

	// The last decrypted segment is cached: parquet readers make many small reads within one segment,
	// and the footer is read right after the last segment is authenticated in NewReaderAt.
	cacheMu         sync.Mutex
	cacheIndex      int64
	cachedPlaintext []byte
}

// NewReaderAt reads the header of an encrypted object of ciphertextSize bytes and authenticates its last segment.
// The last segment is authenticated upfront so that a truncated object fails here rather than in the middle of
// parsing: ciphertextSize comes from the object listing and is otherwise not protected.
func NewReaderAt(src io.ReaderAt, ciphertextSize int64, key *Key) (*ReaderAt, error) {
	plaintextSize, err := PlaintextSize(ciphertextSize)
	if err != nil {
		return nil, ErrAuthentication.Wrap(err)
	}

	header := make([]byte, headerSize)
	if err := readFullAt(src, header, 0); err != nil {
		return nil, xerrors.Errorf("unable to read encryption header: %w", err)
	}
	segmentCipher, err := newSegmentCipher(header, key)
	if err != nil {
		return nil, xerrors.Errorf("unable to read encryption header: %w", err)
	}

	r := &ReaderAt{
		src:             src,
		ciphertextSize:  ciphertextSize,
		plaintextSize:   plaintextSize,
		segmentCount:    segmentCount(ciphertextSize),
		cipher:          segmentCipher,
		cacheMu:         sync.Mutex{},
		cacheIndex:      -1,
		cachedPlaintext: nil,
	}
	if _, err := r.segments(r.segmentCount-1, r.segmentCount-1); err != nil {
		return nil, xerrors.Errorf("unable to authenticate the end of the encrypted object: %w", err)
	}
	return r, nil
}

// Size returns the plaintext size.
func (r *ReaderAt) Size() int64 {
	return r.plaintextSize
}

// ReadAt implements io.ReaderAt over the plaintext. It is safe for concurrent use.
func (r *ReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, xerrors.Errorf("negative offset: %d", off)
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off >= r.plaintextSize {
		return 0, io.EOF
	}
	firstIndex, inner := locatePlaintext(off)
	lastIndex, _ := locatePlaintext(min(off+int64(len(p)), r.plaintextSize) - 1)
	segments, err := r.segments(firstIndex, lastIndex)
	if err != nil {
		return 0, err
	}

	n := 0
	for _, plaintext := range segments {
		n += copy(p[n:], plaintext[inner:])
		inner = 0
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// segments returns decrypted segments [firstIndex, lastIndex]. Their ciphertext is read with one call: every ReadAt
// of an S3 object is a separate HTTP request, so a large read must not turn into a request per segment.
// The returned slices must not be modified.
func (r *ReaderAt) segments(firstIndex int64, lastIndex int64) ([][]byte, error) {
	segments := make([][]byte, 0, lastIndex-firstIndex+1)
	// Sequential reads start in the segment where the previous read ended.
	if plaintext, ok := r.cached(firstIndex); ok {
		segments = append(segments, plaintext)
		firstIndex++
	}
	if firstIndex > lastIndex {
		return segments, nil
	}

	start, _ := segmentBounds(firstIndex, r.ciphertextSize)
	_, end := segmentBounds(lastIndex, r.ciphertextSize)
	ciphertext := make([]byte, end-start)
	if err := readFullAt(r.src, ciphertext, start); err != nil {
		return nil, xerrors.Errorf("unable to read encrypted segments: %w", err)
	}

	for index := firstIndex; index <= lastIndex; index++ {
		segmentStart, segmentEnd := segmentBounds(index, r.ciphertextSize)
		segmentCiphertext := ciphertext[segmentStart-start : segmentEnd-start]
		// Decrypt in place: the ciphertext buffer is not used afterwards.
		plaintext, err := r.cipher.open(segmentCiphertext[:0], segmentCiphertext, index, index == r.segmentCount-1)
		if err != nil {
			return nil, xerrors.Errorf("unable to decrypt segment %d of %d: %w", index+1, r.segmentCount, err)
		}
		segments = append(segments, plaintext)
	}
	last := segments[len(segments)-1]
	if firstIndex < lastIndex {
		// Segments decrypted together share one buffer: the cache must not keep the whole buffer of a large read.
		last = bytes.Clone(last)
	}
	r.cache(lastIndex, last)
	return segments, nil
}

func (r *ReaderAt) cached(index int64) ([]byte, bool) {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()
	if r.cacheIndex != index {
		return nil, false
	}
	return r.cachedPlaintext, true
}

func (r *ReaderAt) cache(index int64, plaintext []byte) {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()
	r.cacheIndex = index
	r.cachedPlaintext = plaintext
}

// readFullAt reads exactly len(p) bytes. io.ReaderAt may return io.EOF together with a full buffer at the end of
// the source, which is not an error here.
func readFullAt(src io.ReaderAt, p []byte, off int64) error {
	n, err := src.ReadAt(p, off)
	if n == len(p) {
		return nil
	}
	if err == nil || xerrors.Is(err, io.EOF) {
		return xerrors.Errorf("unexpected end of object: read %d of %d bytes at offset %d", n, len(p), off)
	}
	return err
}
