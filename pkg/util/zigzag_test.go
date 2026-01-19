package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestZigzagVarIntDecode(t *testing.T) {
	check := func(t *testing.T, inBuf []byte, expectedResult int64, expectedConsumed int) {
		result, consumed, err := ZigzagVarIntDecode(inBuf)
		require.NoError(t, err)
		require.Equal(t, expectedResult, result)
		require.Equal(t, expectedConsumed, consumed)
	}

	check(t, []byte{0x02}, 1, 1)

	check(t, []byte{0x00}, 0, 1)
	check(t, []byte{0x02}, 1, 1)
	check(t, []byte{0x01}, -1, 1)
	check(t, []byte{0x04}, 2, 1)
	check(t, []byte{0x03, 0x01}, -2, 1)
	check(t, []byte{0x08}, 4, 1)
	check(t, []byte{0x07, 0x01}, -4, 1)
	check(t, []byte{0x80, 0x01}, 64, 2)
	check(t, []byte{0x80, 0x02}, 128, 2)
	check(t, []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}, 36028797018963968, 9)
}

func TestZigzagDecode(t *testing.T) {
	tests := []struct {
		name     string
		input    uint64
		expected int64
	}{
		{"zero", 0, 0},
		{"positive_one", 2, 1},
		{"negative_one", 1, -1},
		{"positive_two", 4, 2},
		{"negative_two", 3, -2},
		{"positive_four", 8, 4},
		{"negative_four", 7, -4},
		{"max_positive", 18446744073709551614, 9223372036854775807},
		{"min_negative", 18446744073709551615, -9223372036854775808},
		{"large_positive", 16, 8},
		{"large_negative", 15, -8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ZigzagDecode(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}
