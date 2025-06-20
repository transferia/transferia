//go:build !disable_clickhouse_provider

package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBufWithPos(t *testing.T) {
	buf := NewBufWithPos()

	buf.RememberPos()
	_, _ = buf.Write([]byte("a"))
	_, _ = buf.Write([]byte("b"))
	_, _ = buf.Write([]byte("\n"))
	bytes := buf.BufFromRememberedPos()
	require.Equal(t, []byte("ab\n"), bytes)

	buf.RememberPos()
	_, _ = buf.Write([]byte("c"))
	_, _ = buf.Write([]byte("\n"))
	bytes2 := buf.BufFromRememberedPos()
	require.Equal(t, []byte("c\n"), bytes2)
}
