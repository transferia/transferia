package util

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const minReadBufferSize = 16 // https://github.com/transferria/transferria/arc_vcs/contrib/go/_std/src/bufio/bufio.go?rev=r9417606#L41

func TestScannerBasic(t *testing.T) {
	builder := strings.Builder{}
	scanner := NewLineSplitterSize(
		bytes.NewBuffer(bytes.Join([][]byte{
			[]byte("a"),
			[]byte("b"),
		}, []byte("\n"))),
		&builder,
		minReadBufferSize,
	)

	require.NoError(t, scanner.ConsumeRow())
	require.EqualValues(t, "a\n", builder.String())
	builder.Reset()

	require.Equal(t, io.EOF, scanner.ConsumeRow())
	require.EqualValues(t, "b", builder.String())
}

func TestScannerBiggerLines(t *testing.T) {
	builder := strings.Builder{}
	scanner := NewLineSplitterSize(bytes.NewBuffer(
		bytes.Join([][]byte{
			[]byte("12345678901234567890"),
			[]byte("12345\n"),
		}, []byte("\n"))),
		&builder,
		minReadBufferSize,
	)

	require.NoError(t, scanner.ConsumeRow())
	require.EqualValues(t, "12345678901234567890\n", builder.String())
	builder.Reset()

	require.NoError(t, scanner.ConsumeRow())
	require.EqualValues(t, "12345\n", builder.String())
	builder.Reset()

	require.EqualValues(t, io.EOF, scanner.ConsumeRow())
	require.Empty(t, builder.String())
}

func TestScannerQuotes(t *testing.T) {
	builder := strings.Builder{}
	scanner := NewLineSplitterSize(
		bytes.NewBuffer(bytes.Join([][]byte{
			[]byte(`"234567890123456789"`),
			[]byte(`"2345"` + "\n"),
		}, []byte("\n"))),
		&builder,
		minReadBufferSize,
	)

	require.NoError(t, scanner.ConsumeRow())
	require.EqualValues(t, `"234567890123456789"`+"\n", builder.String())
	builder.Reset()

	require.NoError(t, scanner.ConsumeRow())
	require.EqualValues(t, `"2345"`+"\n", builder.String())
	builder.Reset()

	require.EqualValues(t, io.EOF, scanner.ConsumeRow())
	require.Empty(t, builder.String())
}

//---

type stringsBuilderWithCounter struct {
	callsCounter int
}

func (m *stringsBuilderWithCounter) Write(p []byte) (n int, err error) {
	m.callsCounter++
	return len(p), nil
}

func TestLongLineWritesPartByPart(t *testing.T) {
	stringsBuilder := stringsBuilderWithCounter{
		callsCounter: 0,
	}
	numParts := 7
	scanner := NewLineSplitterSize(
		bytes.NewBuffer(bytes.Join([][]byte{
			[]byte(strings.Repeat("a", minReadBufferSize*numParts)),
			[]byte("z"),
		}, []byte("\n"))),
		&stringsBuilder,
		minReadBufferSize,
	)
	require.NoError(t, scanner.ConsumeRow())
	require.Equal(t, numParts+1, stringsBuilder.callsCounter) // +1 bcs '\n' goes as the last byte
}
