package errors

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/transferia/transferia/library/go/core/xerrors"
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
