package table_part_provider

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/util/jsonx"
)

func TestAddKeyToJson(t *testing.T) {
	testCases := []struct {
		input    string
		expected map[string]any
	}{
		{
			input:    ``,
			expected: map[string]any{"key": json.Number("123")},
		},
		{
			input:    `{"key-1":"text"}`,
			expected: map[string]any{"key-1": "text", "key": json.Number("123")},
		},
	}

	for i, testCase := range testCases {
		res, err := addKeyToJson(testCase.input, "key", 123)
		require.NoError(t, err)
		var actual map[string]any
		require.NoError(t, jsonx.Unmarshal(res, &actual))
		require.Equal(t, testCase.expected, actual, fmt.Sprintf("test-case-%d", i))
	}
}
