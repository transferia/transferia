//go:build !disable_ydb_provider

package ydb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMakeTablePath(t *testing.T) {
	require.Equal(t, "a/b", makeTablePathFromTopicPath("/local/a/b/dtt", "dtt", "local"))
	require.Equal(t, "cashbacks", makeTablePathFromTopicPath("/ru-central1/b1gnusj8glj8pkr3ru0e/etn01jlrd2bfp06votrk/cashbacks/dtt", "dtt", "/ru-central1/b1gnusj8glj8pkr3ru0e/etn01jlrd2bfp06votrk"))
}
