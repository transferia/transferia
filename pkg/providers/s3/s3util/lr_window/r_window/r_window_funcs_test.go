package r_window

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/file"
)

func TestGetRightWindow(t *testing.T) {
	overlapWindowTimeSize := 10 * time.Minute

	rWindow, err := buildRightWindow([]file.File{
		{FileName: "A", LastModifiedNS: 0},
		{FileName: "B", LastModifiedNS: overlapWindowTimeSize.Nanoseconds() - 1},
		{FileName: "C", LastModifiedNS: overlapWindowTimeSize.Nanoseconds()},
		{FileName: "D", LastModifiedNS: overlapWindowTimeSize.Nanoseconds() + 1},
		{FileName: "E", LastModifiedNS: 2*overlapWindowTimeSize.Nanoseconds() - 1},
		{FileName: "F", LastModifiedNS: 2 * overlapWindowTimeSize.Nanoseconds()},
		{FileName: "G", LastModifiedNS: 2*overlapWindowTimeSize.Nanoseconds() + 1},
	}, 0, overlapWindowTimeSize)
	require.NoError(t, err)
	require.Equal(t, 3, rWindow.TotalSize())
}
