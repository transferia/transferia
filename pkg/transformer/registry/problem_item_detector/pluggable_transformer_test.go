package problemitemdetector

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

type mockSink struct {
	abstract.Sinker
	pushWithError bool
}

func (s *mockSink) Push([]abstract.ChangeItem) error {
	if s.pushWithError {
		return errors.New("error")
	}
	return nil
}

func TestPluggableTransformer(t *testing.T) {
	q := map[string]string{"q": "q"}
	items := []abstract.ChangeItem{{
		ColumnNames:  []string{"a"},
		ColumnValues: []any{q},
	}}

	transformer := newPluggableTransformer(&mockSink{pushWithError: false})
	err := transformer.Push(items)
	require.NoError(t, err)

	transformer = newPluggableTransformer(&mockSink{pushWithError: true})
	err = transformer.Push(items)
	require.Error(t, err)
	require.True(t, abstract.IsFatal(err))
}
