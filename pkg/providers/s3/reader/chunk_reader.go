package reader

import (
	"io"

	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	DefaultChunkReaderBlockSize = 20 * humanize.MiByte
	GrowFactor                  = 1.5 // 50% of the current buffer size
)

// ChunkReader is a reader that reads chunks from a some reader
// buff length is always equal to maxBuffSize
type ChunkReader struct {
	buff        []byte
	maxBuffSize int
	offset      int64
	reader      io.ReadCloser
	used        int
	foundEOF    bool
	logger      log.Logger
}

// ReadNextChunk reads the next chunk from the reader
// if the reader is at the end of the file, it sets the foundEOF flag to true
func (r *ChunkReader) ReadNextChunk() error {
	if r.used == r.maxBuffSize {
		oldBuff := r.buff[:r.used]
		r.maxBuffSize = int(float64(r.maxBuffSize) * GrowFactor) // increase buffer size by GrowFactor of the current buffer size
		r.buff = make([]byte, r.maxBuffSize)
		r.FillBuffer(oldBuff)

		r.logger.Infof("ChunkReader buff increased from %s to %s", humanize.Bytes(uint64(r.used)), humanize.Bytes(uint64(r.maxBuffSize)))
	}

	for r.used < r.maxBuffSize && !r.foundEOF {
		if err := r.read(); err != nil {
			return xerrors.Errorf("failed to read chunk: %w", err)
		}
	}

	return nil
}

func (r *ChunkReader) read() error {
	read, err := r.reader.Read(r.buff[r.used:])
	if err != nil && !xerrors.Is(err, io.EOF) {
		return err
	}
	if err != nil && xerrors.Is(err, io.EOF) {
		r.foundEOF = true
	}
	if read == 0 {
		r.foundEOF = true
	}

	r.used += read
	r.offset += int64(read)

	return nil
}

// FillBuffer fills the buffer with the data that was read from the reader
// if the buffer is not large enough, it will be resized to the size of the data
// if the buffer is large enough, it will be copied to the buffer
func (r *ChunkReader) FillBuffer(data []byte) {
	if len(data) > r.maxBuffSize {
		r.buff = make([]byte, len(data))
		r.maxBuffSize = len(data)
	}
	copy(r.buff, data)
	r.used = len(data)
}

// Data returns the data read from the reader without copying it
// if you need to change the data in different places copy it to another slice
func (r *ChunkReader) Data() []byte {
	return r.buff[:r.used]
}

func (r *ChunkReader) IsEOF() bool {
	return r.foundEOF
}

func (r *ChunkReader) Close() error {
	if r.reader == nil {
		return nil
	}
	if err := r.reader.Close(); err != nil {
		return err
	}
	r.reader = nil
	return nil
}

func (r *ChunkReader) Offset() int64 {
	return r.offset
}

// if maxBuffSize is 0, use DefaultChunkReaderBlockSize
func NewChunkReader(reader io.ReadCloser, maxBuffSize int, logger log.Logger) *ChunkReader {
	if maxBuffSize == 0 {
		maxBuffSize = DefaultChunkReaderBlockSize
	}
	return &ChunkReader{
		buff:        make([]byte, maxBuffSize),
		maxBuffSize: maxBuffSize,
		offset:      0,
		reader:      reader,
		used:        0,
		foundEOF:    false,
		logger:      logger,
	}
}
