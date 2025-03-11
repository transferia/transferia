package problemitemdetector

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

type mockLogger struct {
	log.Logger
	errorCounter  int
	errorfCounter int
}

func (l *mockLogger) Error(msg string, fields ...log.Field) {
	l.errorCounter++
}

func (l *mockLogger) Errorf(msg string, args ...interface{}) {
	l.errorfCounter++
}

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
	logger := &mockLogger{}
	q := map[string]string{"q": "q"}
	items := []abstract.ChangeItem{{
		ColumnNames:  []string{"a"},
		ColumnValues: []interface{}{q},
	}}

	transformer := newPluggableTransformer(&mockSink{pushWithError: false}, logger)
	err := transformer.Push(items)
	require.NoError(t, err)
	require.Equal(t, 0, logger.errorCounter)

	transformer = newPluggableTransformer(&mockSink{pushWithError: true}, logger)
	err = transformer.Push(items)
	require.Error(t, err)
	require.Equal(t, 1, logger.errorCounter)
	require.Equal(t, 1, logger.errorfCounter)
}
