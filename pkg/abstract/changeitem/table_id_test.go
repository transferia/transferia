package changeitem

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIncludesExactOnly covers P1#10: Includes is exact-name matching (plus
// the "*" wildcard). A directory-style receiver does NOT include tables under
// it — hierarchical providers use FilteredTableLister for directory includes.
func TestIncludesExactOnly(t *testing.T) {
	dir := TableID{Name: "a/b"}
	sub := TableID{Name: "a/b/c"}
	require.False(t, dir.Includes(sub), "directory-style prefix includes are not supported")

	require.True(t, dir.Includes(TableID{Name: "a/b"}), "exact match")
	require.True(t, TableID{Name: "*"}.Includes(TableID{Name: "anything"}), "star matches anything")

	require.True(t, TableID{Namespace: "ns", Name: "*"}.Includes(TableID{Namespace: "ns", Name: "t"}), "namespace star matches")
	require.True(t, TableID{Namespace: "ns", Name: "t"}.Includes(TableID{Namespace: "ns", Name: "t"}), "namespace exact match")
	require.False(t, TableID{Namespace: "ns", Name: "t"}.Includes(TableID{Namespace: "other", Name: "t"}), "different namespaces do not match")
}
