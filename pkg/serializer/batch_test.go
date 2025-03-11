package serializer

import (
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
)

type dummySerializer struct {
	hook func()
}

func (s *dummySerializer) Serialize(item *abstract.ChangeItem) ([]byte, error) {
	if s.hook != nil {
		s.hook()
	}

	return strconv.AppendUint(nil, item.LSN, 10), nil
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
