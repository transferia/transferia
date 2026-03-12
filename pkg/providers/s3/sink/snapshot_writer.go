package sink

import (
	"context"
	"io"
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/serializer"
)

// SnapshotWriter manages the lifecycle of writing snapshot data to S3.
// It coordinates serialization, streaming through a pipe, and synchronization
// with the async S3 upload process.
type SnapshotWriter struct {
	ctx        context.Context
	cancel     context.CancelFunc
	key        string
	serializer serializer.BatchSerializer
	writer     io.WriteCloser
	uploadDone chan error

	uploadOnce sync.Once
}

// Close finalizes the snapshot by closing the serializer and writer,
// then waits for the async S3 upload to complete. This method blocks
// until the upload finishes or fails.
func (s *SnapshotWriter) Close() error {
	if s == nil {
		return nil
	}

	lastBytes, err := s.serializer.Close()
	if err != nil {
		return xerrors.Errorf("unable to close serializer: %w", err)
	}
	if len(lastBytes) != 0 {
		if _, err = s.writer.Write(lastBytes); err != nil {
			return xerrors.Errorf("unable to write last bytes to snapshot: %w", err)
		}
	}
	if err := s.writer.Close(); err != nil {
		return xerrors.Errorf("unable to close writer: %w", err)
	}

	// Wait for upload to complete - blocking read until finishUpload() closes the channel
	uploadErr := <-s.uploadDone
	if uploadErr != nil {
		return xerrors.Errorf("error during upload: %w", uploadErr)
	}
	return nil
}

// Write serializes and writes a batch of change items to the snapshot file.
// Returns the number of bytes written.
func (s *SnapshotWriter) Write(items []*abstract.ChangeItem) (int, error) {
	written, err := s.serializer.SerializeAndWrite(s.ctx, items, s.writer)
	if err != nil {
		return 0, xerrors.Errorf("unable to serialize and write: %w", err)
	}
	return written, nil
}

// FinishUpload signals the completion of the async S3 upload operation.
// It sends the upload result (success or error) through the uploadDone channel
// and cancels the context. This method is called by the upload goroutine.
func (s *SnapshotWriter) FinishUpload(err error) {
	s.uploadOnce.Do(func() {
		s.uploadDone <- err
		close(s.uploadDone)
	})
	s.cancel()
}

// NewsnapshotWriter creates a new snapshotWriter instance for writing snapshot data.
// It sets up the serializer, writer, and synchronization primitives needed for
// coordinating the write and upload operations.
func NewsnapshotWriter(
	ctx context.Context,
	serializer serializer.BatchSerializer,
	writer io.WriteCloser,
	key string,
) (*SnapshotWriter, error) {
	ctx, cancel := context.WithCancel(ctx)

	holder := &SnapshotWriter{
		ctx:        ctx,
		cancel:     cancel,
		key:        key,
		serializer: serializer,
		uploadOnce: sync.Once{},
		uploadDone: make(chan error),
		writer:     writer,
	}

	return holder, nil
}
