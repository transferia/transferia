//go:build !disable_s3_provider

package sink

import (
	"bytes"
	"compress/gzip"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGzipSnapshot(t *testing.T) {
	rdr := NewSnapshotGzip()
	line := "line of data\n"
	repeats := 100
	go func() {
		for i := 0; i < repeats; i++ {
			rdr.FeedChannel() <- []byte(strings.Repeat(line, 1000))
		}
		rdr.Close()
	}()
	data, err := io.ReadAll(rdr)
	require.NoError(t, err)
	dec, err := gzip.NewReader(bytes.NewReader(data))
	require.NoError(t, err)
	decoded, err := io.ReadAll(dec)
	require.NoError(t, err)
	require.True(t, strings.Contains(string(decoded), "line of data"))
	require.Equal(t, len(decoded), repeats*1000*len(line))
}
