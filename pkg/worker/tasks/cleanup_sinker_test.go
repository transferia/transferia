package tasks

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

func TestLeftDiff(t *testing.T) {
	left := make(map[abstract.TableID][]abstract.TableID)
	left[id("public", "only_left")] = []abstract.TableID{
		id("public", "only_left_1"),
		id("public", "only_left_2"),
	}
	left[id("public", "partially_intersects")] = []abstract.TableID{
		id("public", "only_left_3"),
		id("public", "both_1"),
	}
	left[id("public", "completely_intersects")] = []abstract.TableID{
		id("public", "both_2"),
		id("public", "both_3"),
	}
	right := make(map[abstract.TableID][]abstract.TableID)
	right[id("public", "only_right")] = []abstract.TableID{
		id("public", "only_right_1"),
	}
	right[id("public", "partially_intersects")] = []abstract.TableID{
		id("public", "only_right_1"),
		id("public", "both_1"),
	}
	right[id("public", "completely_intersects")] = []abstract.TableID{
		id("public", "both_2"),
		id("public", "both_3"),
	}

	leftOnly, intersect := leftDiff(left, right)

	require.True(t, len(intersect) == 2)
	partiallyIntersects, partiallyIntersectsOk := intersect[id("public", "partially_intersects")]
	require.True(t, partiallyIntersectsOk)
	require.True(t, len(partiallyIntersects) == 1)
	require.True(t, partiallyIntersects[0] == id("public", "both_1"))

	completelyIntersects, completelyIntersectsOk := intersect[id("public", "completely_intersects")]
	require.True(t, completelyIntersectsOk)
	require.True(t, len(completelyIntersects) == 2)

	require.True(t, len(leftOnly) == 2)

	publicOnlyLeft, onlyOk := leftOnly[id("public", "only_left")]
	require.True(t, onlyOk)
	require.True(t, len(publicOnlyLeft) == 2)

	partiallyDiff, partiallyOk := leftOnly[id("public", "partially_intersects")]
	require.True(t, partiallyOk)
	require.True(t, len(partiallyDiff) == 1)
	require.True(t, partiallyDiff[0] == id("public", "only_left_3"))
}

func id(namespace, name string) abstract.TableID {
	return abstract.TableID{
		Namespace: namespace,
		Name:      name,
	}
}
