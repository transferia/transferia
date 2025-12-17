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

func TestReadAllMultilineLines_WithTrailingNewlines(t *testing.T) {
	obj1 := `{
	"a": 1,
	"b": {
		"c": 2
	}
}`

	obj2 := `{
	"name": "test",
	"nested": { "a": { "b": 3 } },
	"arr": [1, 2, {"k": "c"}]
}`

	content := []byte(obj1 + "\n" + obj2 + "\n")
	lines, readBytes := readAllMultilineLines(content)

	require.Equal(t, 2, len(lines))
	require.Equal(t, obj1, lines[0])
	require.Equal(t, obj2, lines[1])
	require.Equal(t, len(content), readBytes)

	// safe slicing
	require.NotPanics(t, func() { _ = content[readBytes:] })
}

func TestReadAllMultilineLines_LastLineWithoutTrailingNewline(t *testing.T) {
	obj1 := `{
	"id": 42
}`
	obj2 := `{
	"payload": {"a": 1, "b": [2,3]},
	"text": "many
lines"
}`
	content := []byte(obj1 + "\n" + obj2)
	lines, readBytes := readAllMultilineLines(content)

	require.Equal(t, 2, len(lines))
	require.Equal(t, obj1, lines[0])
	require.Equal(t, obj2, lines[1])
	require.Equal(t, len(content), readBytes)

	// safe slicing
	require.NotPanics(t, func() { _ = content[readBytes:] })
}

func TestReadAllMultilineLines_EmptyContent(t *testing.T) {
	content := []byte("")
	lines, readBytes := readAllMultilineLines(content)

	require.Equal(t, 0, len(lines))
	require.Equal(t, 0, readBytes)
}

func TestReadAllMultilineLines_InvalidContent(t *testing.T) {
	content := []byte("invalid}")
	lines, readBytes := readAllMultilineLines(content)

	require.Equal(t, 0, len(lines))
	require.Equal(t, 0, readBytes)
}

func TestReadAllMultilineLines_CurlyBracketsInTheValue(t *testing.T) {
	t.Run("simple case", func(t *testing.T) {
		content := []byte(`{"value": "{{some text}}}}}}]]]]]{{}}"}`)
		lines, readBytes := readAllMultilineLines(content)
		require.Equal(t, 1, len(lines))
		require.Equal(t, `{"value": "{{some text}}}}}}]]]]]{{}}"}`, lines[0])
		require.Equal(t, len(content), readBytes)
	})

	t.Run("curly brackets in the value with quotes", func(t *testing.T) {
		content := []byte(`{"value": "{{some text\"}\"}}}}}]]]]]{{}}"}`) // here \" is not a part of the json
		lines, readBytes := readAllMultilineLines(content)
		require.Equal(t, 1, len(lines))
		require.Equal(t, `{"value": "{{some text\"}\"}}}}}]]]]]{{}}"}`, lines[0])
		require.Equal(t, len(content), readBytes)
	})
}
