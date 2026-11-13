package serializer

import (
	"bytes"
	"context"
	"errors"
	"io"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

type dummySerializer struct {
	Serializer
	hook func()
}

func (s *dummySerializer) SerializeWithSeparatorTo(item *abstract.ChangeItem, separator []byte, buf *bytes.Buffer) error {
	data, err := s.Serialize(item)
	if err != nil {
		return xerrors.Errorf("dummySerializer: unable to serialize item: %w", err)
	}
	if _, err := buf.Write(data); err != nil {
		return xerrors.Errorf("dummySerializer: unable to write data to buffer: %w", err)
	}
	if _, err := buf.Write(separator); err != nil {
		return xerrors.Errorf("dummySerializer: unable to write separator: %w", err)
	}
	return nil
}

func (s *dummySerializer) Serialize(item *abstract.ChangeItem) ([]byte, error) {
	if s.hook != nil {
		s.hook()
	}

	return strconv.AppendUint(nil, item.LSN, 10), nil
}

func (s *dummySerializer) Close() ([]byte, error) {
	return nil, nil
}

func TestBatchSerializer(t *testing.T) {
	separator := []byte("||")

	items := make([]*abstract.ChangeItem, 100)
	for i := range items {
		items[i] = &abstract.ChangeItem{
			LSN: uint64(i),
		}
	}

	sequential := newBatchSerializer(
		&dummySerializer{},
		separator,
		&BatchSerializerConfig{
			DisableConcurrency: true,
		},
	)

	expected, err := sequential.Serialize(items)
	require.NoError(t, err)

	concurrent := newBatchSerializer(
		&dummySerializer{hook: runtime.Gosched},
		separator,
		&BatchSerializerConfig{
			Concurrency: 7,
			Threshold:   3,
		},
	)

	actual, err := concurrent.Serialize(items)
	require.NoError(t, err)

	require.Equal(t,
		expected,
		actual,
	)
}

func TestBatchSerializerSerializeAndWriteCanceledContext(t *testing.T) {
	s := newBatchSerializer(&dummySerializer{}, nil, &BatchSerializerConfig{DisableConcurrency: true}).(*batchSerializer)
	// The canceled context makes the pool return nil once it is empty.
	require.NotNil(t, s.bufferPool.Get(context.Background()))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var out bytes.Buffer
	_, err := s.SerializeAndWrite(ctx, []*abstract.ChangeItem{{LSN: 1}}, &out)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, out.Len())
}

func TestBatchSerializerSerializeAndWriteRecoversAfterWriteError(t *testing.T) {
	t.Parallel()

	s := newBatchSerializer(&dummySerializer{}, []byte(","), &BatchSerializerConfig{
		Concurrency: 2,
		Threshold:   1,
	})
	items := []*abstract.ChangeItem{{LSN: 1}, {LSN: 2}, {LSN: 3}, {LSN: 4}}
	writeErr := errors.New("snapshot write failed")
	pipeReader, pipeWriter := io.Pipe()
	require.NoError(t, pipeReader.CloseWithError(writeErr))

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	// Exercise repeated failures because dropping a buffer on cancellation depends on scheduling.
	for range 32 {
		_, err := s.SerializeAndWrite(ctx, items, pipeWriter)
		assert.ErrorIs(t, err, writeErr)

		var out bytes.Buffer
		written, err := s.SerializeAndWrite(ctx, items, &out)
		require.NoError(t, err, "the next batch must be able to use the same serializer")
		require.Equal(t, "1,2,3,4", out.String())
		require.Equal(t, 7, written)
	}
}
