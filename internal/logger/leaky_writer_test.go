package logger

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/library/go/core/metrics/solomon"
	"github.com/transferria/transferria/pkg/util/size"
)

type fakeWriter struct {
	t          *testing.T
	canWrite   bool
	afterWrite func(w *fakeWriter)
	data       [][]byte
}

func newFakeWriter(t *testing.T, canWrite bool, afterWrite func(w *fakeWriter)) *fakeWriter {
	return &fakeWriter{t: t, canWrite: canWrite, afterWrite: afterWrite}
}

func (w *fakeWriter) Write(p []byte) (int, error) {
	if !w.canWrite {
		require.Fail(w.t, "invalid write call")
	}
	w.data = append(w.data, p)
	if w.afterWrite != nil {
		w.afterWrite(w)
	}
	return len(p), nil
}

func (w *fakeWriter) CanWrite() bool {
	return w.canWrite
}

func TestLeakyWriter(t *testing.T) {
	writer := newFakeWriter(t, false, nil)
	leakyWriter := NewLeakyWriter(writer, solomon.NewRegistry(solomon.NewRegistryOpts()), size.GiB)
	leakedCount := 10
	leakedSize := 0
	for i := 0; i < leakedCount; i++ {
		msg := fmt.Sprintf("message %v", i)
		leakedSize += len(msg)
		n, err := leakyWriter.Write([]byte(msg))
		require.NoError(t, err)
		require.Equal(t, len(msg), n)
	}
	require.Equal(t, leakedCount, leakyWriter.leakedCount)
	require.Equal(t, leakedSize, leakyWriter.leakedSize)
	writer.canWrite = true
	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("message %v", i)
		n, err := leakyWriter.Write([]byte(msg))
		require.NoError(t, err)
		require.Equal(t, len(msg), n)
		require.Equal(t, msg, string(writer.data[i+1]))
	}
	require.Equal(t, fmt.Sprintf(leakageInfoMsg, leakedCount, leakedSize), string(writer.data[0]))
	require.Equal(t, 0, leakyWriter.leakedCount)
	require.Equal(t, 0, leakyWriter.leakedSize)
}

func TestLeakyWriter_WriteAfterLeakageInfo(t *testing.T) {
	writer := newFakeWriter(t, false, func(w *fakeWriter) {
		w.canWrite = false
	})
	leakyWriter := NewLeakyWriter(writer, solomon.NewRegistry(solomon.NewRegistryOpts()), size.GiB)

	m1 := "some message"
	n, err := leakyWriter.Write([]byte(m1))
	require.NoError(t, err)
	require.Equal(t, len(m1), n)
	require.Equal(t, 1, leakyWriter.leakedCount)
	require.Equal(t, len(m1), leakyWriter.leakedSize)

	writer.canWrite = true
	m2 := "message after leakage info"
	n, err = leakyWriter.Write([]byte(m2))
	require.NoError(t, err)
	require.Equal(t, len(m2), n)
	require.Equal(t, 1, leakyWriter.leakedCount)
	require.Equal(t, len(m2), leakyWriter.leakedSize)
	require.Equal(t, 1, len(writer.data))
	require.Equal(t, fmt.Sprintf(leakageInfoMsg, 1, len(m1)), string(writer.data[0]))
}

func TestLeakyWriter_MaxSize(t *testing.T) {
	writer := newFakeWriter(t, true, nil)
	leakyWriter := NewLeakyWriter(writer, solomon.NewRegistry(solomon.NewRegistryOpts()), size.KiB)

	m1 := make([]byte, size.KiB+1)
	n, err := leakyWriter.Write(m1)
	require.NoError(t, err)
	require.Equal(t, len(m1), n)
	require.Equal(t, 1, leakyWriter.leakedCount)
	require.Equal(t, len(m1), leakyWriter.leakedSize)
	require.Equal(t, 0, len(writer.data))

	m2 := make([]byte, size.KiB)
	n, err = leakyWriter.Write(m2)
	require.NoError(t, err)
	require.Equal(t, len(m2), n)
	require.Equal(t, 2, len(writer.data))
	require.Equal(t, fmt.Sprintf(leakageInfoMsg, 1, len(m1)), string(writer.data[0]))
	require.Equal(t, m2, writer.data[1])
}
