package sink

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

// mockSerializer is a mock implementation of serializer.BatchSerializer for testing
type mockSerializer struct {
	serializeFunc         func(items []*abstract.ChangeItem) ([]byte, error)
	serializeAndWriteFunc func(ctx context.Context, items []*abstract.ChangeItem, writer io.Writer) (int, error)
	closeFunc             func() ([]byte, error)
}

func (m *mockSerializer) Serialize(items []*abstract.ChangeItem) ([]byte, error) {
	if m.serializeFunc != nil {
		return m.serializeFunc(items)
	}
	return []byte("serialized"), nil
}

func (m *mockSerializer) SerializeAndWrite(ctx context.Context, items []*abstract.ChangeItem, writer io.Writer) (int, error) {
	if m.serializeAndWriteFunc != nil {
		return m.serializeAndWriteFunc(ctx, items, writer)
	}
	data := []byte("test data")
	n, err := writer.Write(data)
	return n, err
}

func (m *mockSerializer) Close() ([]byte, error) {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil, nil
}

// mockWriteCloser is a mock implementation of io.WriteCloser for testing
type mockWriteCloser struct {
	buf       *bytes.Buffer
	writeFunc func(p []byte) (n int, err error)
	closeFunc func() error
	closed    bool
	mu        sync.Mutex
}

func newMockWriteCloser() *mockWriteCloser {
	return &mockWriteCloser{
		buf: &bytes.Buffer{},
	}
}

func (m *mockWriteCloser) Write(p []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.writeFunc != nil {
		return m.writeFunc(p)
	}
	return m.buf.Write(p)
}

func (m *mockWriteCloser) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

func (m *mockWriteCloser) String() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.buf.String()
}

func (m *mockWriteCloser) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// TestNewSnapshotWriter tests the creation of a new snapshotWriter
func TestNewSnapshotWriter(t *testing.T) {
	ctx := context.Background()
	mockSer := &mockSerializer{}
	mockWriter := newMockWriteCloser()
	key := "test-key"

	writer, err := newsnapshotWriter(ctx, mockSer, mockWriter, key)

	require.NoError(t, err)
	require.NotNil(t, writer)
	require.Equal(t, key, writer.key)
	require.NotNil(t, writer.ctx)
	require.NotNil(t, writer.cancel)
	require.NotNil(t, writer.serializer)
	require.NotNil(t, writer.writer)
	require.NotNil(t, writer.uploadDone)
}

// TestSnapshotWriter_Write tests the write method
func TestSnapshotWriter_Write(t *testing.T) {
	ctx := context.Background()
	mockWriter := newMockWriteCloser()

	testData := "test write data"
	mockSer := &mockSerializer{
		serializeAndWriteFunc: func(ctx context.Context, items []*abstract.ChangeItem, writer io.Writer) (int, error) {
			n, err := writer.Write([]byte(testData))
			return n, err
		},
	}

	writer, err := newsnapshotWriter(ctx, mockSer, mockWriter, "test-key")
	require.NoError(t, err)

	items := []*abstract.ChangeItem{
		{Kind: abstract.InsertKind, Table: "test_table"},
	}

	bytesWritten, err := writer.write(items)

	require.NoError(t, err)
	require.Equal(t, len(testData), bytesWritten)
	require.Equal(t, testData, mockWriter.String())
}

// TestSnapshotWriter_WriteError tests write method with serializer error
func TestSnapshotWriter_WriteError(t *testing.T) {
	ctx := context.Background()
	mockWriter := newMockWriteCloser()

	expectedErr := errors.New("serialization error")
	mockSer := &mockSerializer{
		serializeAndWriteFunc: func(ctx context.Context, items []*abstract.ChangeItem, writer io.Writer) (int, error) {
			return 0, expectedErr
		},
	}

	writer, err := newsnapshotWriter(ctx, mockSer, mockWriter, "test-key")
	require.NoError(t, err)

	items := []*abstract.ChangeItem{
		{Kind: abstract.InsertKind, Table: "test_table"},
	}

	bytesWritten, err := writer.write(items)

	require.Error(t, err)
	require.Equal(t, 0, bytesWritten)
	require.Contains(t, err.Error(), "unable to serialize and write")
}

// TestSnapshotWriter_Close tests successful close operation
func TestSnapshotWriter_Close(t *testing.T) {
	ctx := context.Background()
	mockWriter := newMockWriteCloser()

	lastBytes := []byte("final data")
	mockSer := &mockSerializer{
		closeFunc: func() ([]byte, error) {
			return lastBytes, nil
		},
	}

	writer, err := newsnapshotWriter(ctx, mockSer, mockWriter, "test-key")
	require.NoError(t, err)

	// Simulate upload completion in goroutine
	go func() {
		time.Sleep(10 * time.Millisecond)
		writer.finishUpload(nil)
	}()

	err = writer.close()

	require.NoError(t, err)
	require.True(t, mockWriter.IsClosed())
	require.Contains(t, mockWriter.String(), string(lastBytes))
}

// TestSnapshotWriter_CloseWithUploadError tests close with upload error
func TestSnapshotWriter_CloseWithUploadError(t *testing.T) {
	ctx := context.Background()
	mockWriter := newMockWriteCloser()
	mockSer := &mockSerializer{}

	writer, err := newsnapshotWriter(ctx, mockSer, mockWriter, "test-key")
	require.NoError(t, err)

	uploadErr := errors.New("upload failed")

	// Simulate upload failure in goroutine
	go func() {
		time.Sleep(10 * time.Millisecond)
		writer.finishUpload(uploadErr)
	}()

	err = writer.close()

	require.Error(t, err)
	require.Contains(t, err.Error(), "error during upload")
	require.Contains(t, err.Error(), "upload failed")
}

// TestSnapshotWriter_CloseWithSerializerError tests close with serializer error
func TestSnapshotWriter_CloseWithSerializerError(t *testing.T) {
	ctx := context.Background()
	mockWriter := newMockWriteCloser()

	serializerErr := errors.New("serializer close error")
	mockSer := &mockSerializer{
		closeFunc: func() ([]byte, error) {
			return nil, serializerErr
		},
	}

	writer, err := newsnapshotWriter(ctx, mockSer, mockWriter, "test-key")
	require.NoError(t, err)

	err = writer.close()

	require.Error(t, err)
	require.Contains(t, err.Error(), "unable to close serializer")
}

// TestSnapshotWriter_CloseWithWriterError tests close with writer error
func TestSnapshotWriter_CloseWithWriterError(t *testing.T) {
	ctx := context.Background()

	writerErr := errors.New("writer close error")
	mockWriter := &mockWriteCloser{
		buf: &bytes.Buffer{},
		closeFunc: func() error {
			return writerErr
		},
	}

	mockSer := &mockSerializer{}

	writer, err := newsnapshotWriter(ctx, mockSer, mockWriter, "test-key")
	require.NoError(t, err)

	err = writer.close()

	require.Error(t, err)
	require.Contains(t, err.Error(), "unable to close writer")
}

// TestSnapshotWriter_CloseNil tests close on nil writer
func TestSnapshotWriter_CloseNil(t *testing.T) {
	var writer *snapshotWriter
	err := writer.close()
	require.NoError(t, err)
}

// TestSnapshotWriter_FinishUpload tests finishUpload method
func TestSnapshotWriter_FinishUpload(t *testing.T) {
	ctx := context.Background()
	mockWriter := newMockWriteCloser()
	mockSer := &mockSerializer{}

	writer, err := newsnapshotWriter(ctx, mockSer, mockWriter, "test-key")
	require.NoError(t, err)

	uploadErr := errors.New("test upload error")

	// Call finishUpload in goroutine
	go func() {
		time.Sleep(10 * time.Millisecond)
		writer.finishUpload(uploadErr)
	}()

	// Wait for upload to finish
	receivedErr := <-writer.uploadDone

	require.Error(t, receivedErr)
	require.Equal(t, uploadErr, receivedErr)
}

// TestSnapshotWriter_FinishUploadMultipleCalls tests that finishUpload is idempotent
func TestSnapshotWriter_FinishUploadMultipleCalls(t *testing.T) {
	ctx := context.Background()
	mockWriter := newMockWriteCloser()
	mockSer := &mockSerializer{}

	writer, err := newsnapshotWriter(ctx, mockSer, mockWriter, "test-key")
	require.NoError(t, err)

	firstErr := errors.New("first error")
	secondErr := errors.New("second error")

	// Call finishUpload multiple times
	go writer.finishUpload(firstErr)
	receivedErr := <-writer.uploadDone
	require.Equal(t, firstErr, receivedErr)

	go writer.finishUpload(secondErr) // Should be ignored due to sync.Once
	time.Sleep(time.Second)           // wait for goroutine to finish
	someErr, ok := <-writer.uploadDone
	require.False(t, ok)
	require.NoError(t, someErr)
}

// TestSnapshotWriter_ContextCancellation tests context cancellation
func TestSnapshotWriter_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mockWriter := newMockWriteCloser()
	mockSer := &mockSerializer{}

	writer, err := newsnapshotWriter(ctx, mockSer, mockWriter, "test-key")
	require.NoError(t, err)

	// Cancel the parent context
	cancel()

	// Writer's context should also be cancelled
	select {
	case <-writer.ctx.Done():
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("context was not cancelled")
	}
}

// TestSnapshotWriter_FinishUploadCancelsContext tests that finishUpload cancels context
func TestSnapshotWriter_FinishUploadCancelsContext(t *testing.T) {
	ctx := context.Background()
	mockWriter := newMockWriteCloser()
	mockSer := &mockSerializer{}

	writer, err := newsnapshotWriter(ctx, mockSer, mockWriter, "test-key")
	require.NoError(t, err)

	go writer.finishUpload(nil)
	require.NoError(t, <-writer.uploadDone)

	// Context should be cancelled after finishUpload
	select {
	case <-writer.ctx.Done():
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("context was not cancelled by finishUpload")
	}
}

// TestSnapshotWriter_WriteWithLastBytes tests close with last bytes from serializer
func TestSnapshotWriter_WriteWithLastBytes(t *testing.T) {
	ctx := context.Background()
	mockWriter := newMockWriteCloser()

	lastBytes := []byte("final chunk")
	mockSer := &mockSerializer{
		closeFunc: func() ([]byte, error) {
			return lastBytes, nil
		},
	}

	writer, err := newsnapshotWriter(ctx, mockSer, mockWriter, "test-key")
	require.NoError(t, err)

	// Write some initial data
	items := []*abstract.ChangeItem{{Kind: abstract.InsertKind, Table: "test"}}
	_, err = writer.write(items)
	require.NoError(t, err)

	// Simulate upload completion
	go func() {
		time.Sleep(10 * time.Millisecond)
		writer.finishUpload(nil)
	}()

	err = writer.close()

	require.NoError(t, err)
	require.Contains(t, mockWriter.String(), string(lastBytes))
}

// TestSnapshotWriter_ConcurrentWrites tests concurrent write operations
func TestSnapshotWriter_ConcurrentWrites(t *testing.T) {
	ctx := context.Background()
	mockWriter := newMockWriteCloser()

	var writeCount int
	var mu sync.Mutex

	mockSer := &mockSerializer{
		serializeAndWriteFunc: func(ctx context.Context, items []*abstract.ChangeItem, writer io.Writer) (int, error) {
			mu.Lock()
			writeCount++
			mu.Unlock()
			data := []byte("data")
			n, err := writer.Write(data)
			return n, err
		},
	}

	writer, err := newsnapshotWriter(ctx, mockSer, mockWriter, "test-key")
	require.NoError(t, err)

	// Perform concurrent writes
	var wg sync.WaitGroup
	numWrites := 10
	wg.Add(numWrites)

	for i := 0; i < numWrites; i++ {
		go func() {
			defer wg.Done()
			items := []*abstract.ChangeItem{{Kind: abstract.InsertKind, Table: "test"}}
			_, _ = writer.write(items)
		}()
	}

	wg.Wait()

	mu.Lock()
	finalCount := writeCount
	mu.Unlock()

	require.Equal(t, numWrites, finalCount)
}
