package errors

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/errors/categories"
)

//go:noinline
func Original() error {
	return xerrors.Errorf("aaaa: %w", errors.New("simple error"))
}

//go:noinline
func Middle() error {
	return xerrors.Errorf("bbb: %w", Original())
}

type MethodTester struct{}

func (m MethodTester) Original() error {
	return xerrors.Errorf("aaaa: %w", errors.New("simple error"))
}

func (m MethodTester) Middle() error {
	return xerrors.Errorf("bbb: %w", m.Original())
}

func TestExtractCodePath(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "error with complex stack trace",
			err:      xerrors.Errorf("roooot %w", Middle()),
			expected: "Original.Middle.TestExtractCodePath",
		},
		{
			name:     "error in struct methods",
			err:      xerrors.Errorf("rooot: %w", MethodTester{}.Middle()),
			expected: "MethodTester.Original.MethodTester.Middle.TestExtractCodePath",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractShortStackTrace(tt.err)
			assert.Equal(t, tt.expected, string(result))
		})
	}
}

func TestLogFatalErrorEmptyMessage(t *testing.T) {
	baseErr := errors.New(`FATAL: database "test_database" does not exist (SQLSTATE 3D000)`)

	wrappedErr1 := xerrors.Errorf("failed to connect to a PostgreSQL instance: %w", baseErr)

	wrappedErr2 := xerrors.Errorf("failed to make a connection pool: %w", wrappedErr1)

	wrappedErr3 := xerrors.Errorf("failed to create a PostgreSQL storage: %w", wrappedErr2)

	categorizedErr := CategorizedErrorf(categories.Source, "failed to resolve storage: unable to create *postgres.PgSource: %w", wrappedErr3)

	message := ExtractShortStackTrace(categorizedErr)
	require.NotEmpty(t, message, "Message should not be empty")
}
