package r_window

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/file"
)

func TestSerDe(t *testing.T) {
	overlapWindowTimeSize := 10 * time.Minute

	rWindow, err := NewRWindowFromFiles(0, 0, []file.File{
		{FileName: "A", LastModifiedNS: 0},
		{FileName: "B", LastModifiedNS: overlapWindowTimeSize.Nanoseconds() - 1},
		{FileName: "C", LastModifiedNS: overlapWindowTimeSize.Nanoseconds()},
		{FileName: "D", LastModifiedNS: overlapWindowTimeSize.Nanoseconds() + 1},
		{FileName: "E", LastModifiedNS: 2*overlapWindowTimeSize.Nanoseconds() - 1},
		{FileName: "F", LastModifiedNS: 2 * overlapWindowTimeSize.Nanoseconds()},
		{FileName: "G", LastModifiedNS: 2*overlapWindowTimeSize.Nanoseconds() + 1},
	})
	require.NoError(t, err)
	serializedBytes, err := rWindow.Serialize()
	require.NoError(t, err)
	state2, err := NewRWindowFromSerialized(0, serializedBytes)
	require.NoError(t, err)
	result, err := state2.Serialize()
	require.NoError(t, err)
	require.Equal(t, serializedBytes, result)
}

func TestIsOutOfRWindow(t *testing.T) {
	overlapWindowTimeSize := 10 * time.Minute

	beforeLabel0 := overlapWindowTimeSize.Nanoseconds() - 1
	label0 := overlapWindowTimeSize.Nanoseconds()
	label1 := overlapWindowTimeSize.Nanoseconds() + 1
	label2 := overlapWindowTimeSize.Nanoseconds() + 2
	label3 := overlapWindowTimeSize.Nanoseconds() + 3
	afterLabel3 := overlapWindowTimeSize.Nanoseconds() + 4

	window, err := NewRWindowFromFiles(overlapWindowTimeSize, 0, []file.File{
		{FileName: "A", LastModifiedNS: label0},
		{FileName: "B", LastModifiedNS: label0},
		{FileName: "C", LastModifiedNS: label1},
		{FileName: "D", LastModifiedNS: label3},
	})
	require.NoError(t, err)

	build := func(fileName string, ts int64) *file.File {
		return &file.File{
			FileName:       fileName,
			LastModifiedNS: ts,
		}
	}

	// checks

	require.False(t, window.IsOutOfRWindow(build("E", beforeLabel0)))

	require.True(t, window.IsOutOfRWindow(build("F", label0))) // in window, unknown
	require.True(t, window.IsOutOfRWindow(build("G", label1))) // in window, unknown
	require.True(t, window.IsOutOfRWindow(build("H", label3))) // in window, unknown

	require.True(t, window.IsOutOfRWindow(build("I", afterLabel3)))

	require.True(t, window.IsOutOfRWindow(build("J", label1)))  // in window on known ts, unknown
	require.True(t, window.IsOutOfRWindow(build("K", label2)))  // in window on unknown ts, unknown
	require.False(t, window.IsOutOfRWindow(build("D", label3))) // in window, known
}

func TestIsOutOfWindow2(t *testing.T) {
	wnd0, err := NewRWindowFromFiles(0, 0, nil)
	require.NoError(t, err)
	require.False(t, wnd0.IsOutOfRWindow(&file.File{}))
	wnd1 := NewRWindowEmpty(0)
	require.False(t, wnd1.IsOutOfRWindow(&file.File{}))
}
