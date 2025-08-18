package reader

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
)

type mockS3RawReader struct {
	s3raw.S3RawReader
	data   []byte
	offset int
	fail   bool
}

func (m *mockS3RawReader) ReadAt(p []byte, off int64) (int, error) {
	if m.fail {
		return 0, errors.New("fail")
	}
	if int(off) >= len(m.data) {
		return 0, io.EOF
	}
	n := copy(p, m.data[off:])
	if int(off)+n >= len(m.data) {
		return n, io.EOF
	}
	return n, nil
}

func (m *mockS3RawReader) LastModified() time.Time {
	return time.Time{}
}

func (m *mockS3RawReader) Read(p []byte) (int, error) {
	n, err := m.ReadAt(p, int64(m.offset))
	m.offset += n

	return n, err
}

func (m *mockS3RawReader) Close() error {
	return nil
}

func TestChunkReader_ReadNextChunk(t *testing.T) {
	data := make([]byte, 21)
	reader := &mockS3RawReader{
		data: data,
	}
	maxBuffSize := len(data) - 1
	cr := NewChunkReader(reader, maxBuffSize, logger.Log)

	err := cr.ReadNextChunk()
	require.NoError(t, err)
	require.Equal(t, maxBuffSize, cr.used)
	require.Equal(t, data[:maxBuffSize], cr.buff[:cr.used])
	require.False(t, cr.foundEOF)

	err = cr.ReadNextChunk()
	require.NoError(t, err)
	require.Equal(t, len(data), cr.used)
	require.Equal(t, data, cr.buff[:cr.used])
	require.True(t, cr.foundEOF)
	require.Equal(t, float64(maxBuffSize)*GrowFactor, float64(cr.maxBuffSize))
}

func TestChunkReader_ReadNextChunk_Error(t *testing.T) {
	reader := &mockS3RawReader{fail: true}
	cr := NewChunkReader(reader, 10, logger.Log)
	err := cr.ReadNextChunk()
	require.Error(t, err)
}

func TestChunkReader_FillBuffer(t *testing.T) {
	cr := NewChunkReader(&mockS3RawReader{}, 10, logger.Log)
	data := []byte("12345")
	cr.FillBuffer(data)
	require.Equal(t, data, cr.buff[:cr.used])
}

func TestChunkReader_ReadData(t *testing.T) {
	cr := NewChunkReader(&mockS3RawReader{}, 10, logger.Log)
	data := []byte("testdata")
	cr.FillBuffer(data)
	out := cr.Data()
	require.Equal(t, data, out)
}

func TestNewChunkReader(t *testing.T) {
	reader := &mockS3RawReader{}
	cr := NewChunkReader(reader, 15, logger.Log)
	require.NotNil(t, cr)
	require.Equal(t, 15, len(cr.buff))
	require.Equal(t, 15, cr.maxBuffSize)
	require.Equal(t, int64(0), cr.offset)
	require.Equal(t, reader, cr.reader)
	require.Equal(t, 0, cr.used)
	require.False(t, cr.foundEOF)
}
