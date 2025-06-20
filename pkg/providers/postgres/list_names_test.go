//go:build !disable_postgres_provider

package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListWithCommaSingleQuoted(t *testing.T) {
	require.Equal(t, `'a', 'b', 'c', 'd'`, ListWithCommaSingleQuoted([]string{"a", "b", "c", "d"}))
	require.Equal(t, `'it', 'don''t', 'mean-a!thing', 'if', 'it', 'ain''t', 'got"that:swing'`, ListWithCommaSingleQuoted([]string{"it", "don't", "mean-a!thing", "if", "it", "ain't", `got"that:swing`}))
}
