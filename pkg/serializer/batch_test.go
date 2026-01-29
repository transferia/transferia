package serializer

import (
	"bytes"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"golang.org/x/xerrors"
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
