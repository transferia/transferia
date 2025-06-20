//go:build !disable_s3_provider

package s3raw

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCalcRange(t *testing.T) {
	buff := make([]byte, 100)
	offset := int64(0)
	totalSize := int64(200)

	// if we start at 0 and want to read 100 bytes we need to fetch from remote bytes in the range 0-99
	start, end, err := calcRange(int64(len(buff)), offset, totalSize)
	require.Equal(t, int64(0), start)
	require.Equal(t, int64(99), end)
	require.NoError(t, err)

	offset = 20 // we have already read 0-19
	// if we start at 20 and want to read 100 bytes we need to fetch from remote bytes in the range 20-119
	start, end, err = calcRange(int64(len(buff)), offset, totalSize)
	require.Equal(t, int64(20), start)
	require.Equal(t, int64(119), end)
	require.NoError(t, err)

	offset = 120 // we have already read 0-119
	// we want to read 100 bytes and start at 120 but we only have 80 bytes left in total to read
	start, end, err = calcRange(int64(len(buff)), offset, totalSize)
	require.Equal(t, int64(120), start)
	require.Equal(t, int64(199), end) // last byte in remote object is at position obj[len(obj)-1] so obj[199]
	require.ErrorIs(t, err, io.EOF)   // we reached the end of remote file so eof is returned

	offset = 230 // offset outside of total file size
	start, end, err = calcRange(int64(len(buff)), offset, totalSize)
	require.Equal(t, int64(0), start)                                // nothing to read
	require.Equal(t, int64(0), end)                                  // nothing to read
	require.ErrorContains(t, err, "offset is bigger than totalSize") // offset was out of possible range

	offset = -2 // negative offset
	start, end, err = calcRange(int64(len(buff)), offset, totalSize)
	require.Equal(t, int64(0), start)
	require.Equal(t, int64(0), end)
	require.ErrorContains(t, err, "offset is negative")
}
