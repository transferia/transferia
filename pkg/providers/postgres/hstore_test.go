//go:build !disable_postgres_provider

package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHstoreToJSON(t *testing.T) {
	result0, err := HstoreToJSON("")
	require.NoError(t, err)
	require.Equal(t, "{}", result0)

	result1, err := HstoreToJSON("{}")
	require.NoError(t, err)
	require.Equal(t, "{}", result1)

	result2, err := HstoreToJSON(`{"a": "b"}`)
	require.NoError(t, err)
	require.Equal(t, `{"a": "b"}`, result2)

	result3, err := HstoreToJSON(`"a"=>"b"`)
	require.NoError(t, err)
	require.Equal(t, `{"a":"b"}`, result3)
}

func TestJSONToHstore(t *testing.T) {
	result0, err := JSONToHstore(`{"a":"b"}`)
	require.NoError(t, err)
	require.Equal(t, `a=>b`, result0)

	result1, err := JSONToHstore(`{"a":null}`)
	require.NoError(t, err)
	require.Equal(t, `a=>NULL`, result1)

	result2, err := JSONToHstore(`{"a":"b", "b":null}`)
	require.NoError(t, err)
	require.True(t, result2 == `b=>NULL,a=>b` || result2 == `a=>b,b=>NULL`)
}
