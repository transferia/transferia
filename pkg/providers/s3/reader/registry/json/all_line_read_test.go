package reader

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
)

// Check that when the last valid JSON line does not end with a newline,
// the current implementation returns bytesRead = len(content) + 1, which causes a panic when slicing the buffer
// as done in the calling code: buff = buff[bytesRead:].
func TestReadAllLines_PanicOnBytesReadGreaterThanBuffer(t *testing.T) {
	const total = 10_000_000

	lineBigToken := bytes.Repeat([]byte("a"), 1_048_575) // token <= scanner limit
	lineBig := append(append([]byte(nil), lineBigToken...), '\n')

	lineRestToken := bytes.Repeat([]byte("b"), 562_813)
	lineRest := append(append([]byte(nil), lineRestToken...), '\n')

	content := make([]byte, 0, total)
	for i := 0; i < 9; i++ {
		content = append(content, lineBig...)
	}
	content = append(content, lineRest...)
	content = append(content, '{', '}') // last line without "\n"

	require.Equal(t, total, len(content))

	_, readBytes, err := readAllLines(content)
	require.NoError(t, err)
	// current implementation counts newline for the last line without "\n"
	// so bytesRead == len(content) + 1
	require.Equal(t, len(content)+1, readBytes) // it's not ok

	// Emulate the place where the buffer is sliced by readBytes, which causes a panic.
	require.Panics(t, func() { _ = content[readBytes:] })
	// should be not panic
	// require.NotPanics(t, func() { _ = content[readBytes:] })
}

// Negative control: if the last line ends with "\n",
// bytesRead does not exceed the buffer length and slicing does not panic.
func TestReadAllLines_NoPanicWhenTrailingNewline(t *testing.T) {
	content := []byte("\n{}\n{}\n{}\n") // last line ends with \n
	lines, readBytes, err := readAllLines(content)
	require.NoError(t, err)
	for _, line := range lines {
		logger.Log.Infof("line: %s", line)
	}
	require.Equal(t, 4, len(lines)) // 3 lines + 1 empty line
	require.Equal(t, len(content), readBytes)

	// safe slicing
	_ = content[readBytes:]
}
