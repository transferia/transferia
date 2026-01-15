package diff

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiff(t *testing.T) {
	t.Run("Test empty diff", func(t *testing.T) {
		res, err := Diff([]string{}, []string{})
		require.NoError(t, err)
		require.Empty(t, res)
	})

	t.Run("Test single left element", func(t *testing.T) {
		res, err := Diff([]string{"a"}, []string{})
		require.NoError(t, err)
		require.Len(t, res, 1)
		require.Equal(t, DiffItem{"a", DiffSourceLeft}, res[0])
	})

	t.Run("Test single right element", func(t *testing.T) {
		res, err := Diff([]string{}, []string{"x"})
		require.NoError(t, err)
		require.Len(t, res, 1)
		require.Equal(t, DiffItem{"x", DiffSourceRight}, res[0])
	})

	t.Run("Test replace of one element", func(t *testing.T) {
		res, err := Diff([]string{"a"}, []string{"x"})
		require.NoError(t, err)
		require.Len(t, res, 2)
		require.Equal(t, DiffItem{"a", DiffSourceLeft}, res[0], "note that left string diffs has priority over right")
		require.Equal(t, DiffItem{"x", DiffSourceRight}, res[1], "note that left string diffs has priority over right")
	})

	t.Run("Test multiple left elements", func(t *testing.T) {
		res, err := Diff([]string{"a", "b", "c"}, []string{})
		require.NoError(t, err)
		require.Len(t, res, 3)
		require.Equal(t, DiffItem{"a", DiffSourceLeft}, res[0])
		require.Equal(t, DiffItem{"b", DiffSourceLeft}, res[1])
		require.Equal(t, DiffItem{"c", DiffSourceLeft}, res[2])
	})

	t.Run("Test multiple right elements", func(t *testing.T) {
		res, err := Diff([]string{}, []string{"x", "y", "z"})
		require.NoError(t, err)
		require.Len(t, res, 3)
		require.Equal(t, DiffItem{"x", DiffSourceRight}, res[0])
		require.Equal(t, DiffItem{"y", DiffSourceRight}, res[1])
		require.Equal(t, DiffItem{"z", DiffSourceRight}, res[2])
	})

	t.Run("Test three left three right common elements", func(t *testing.T) {
		res, err := Diff([]string{"common", "common", "common"}, []string{"common", "common", "common"})
		require.NoError(t, err)
		require.Len(t, res, 3)
		require.Equal(t, DiffItem{"common", DiffSourceBoth}, res[0])
		require.Equal(t, DiffItem{"common", DiffSourceBoth}, res[1])
		require.Equal(t, DiffItem{"common", DiffSourceBoth}, res[2])
	})

	t.Run("Test three left three right different elements", func(t *testing.T) {
		res, err := Diff([]string{"a", "b", "c"}, []string{"x", "y", "z"})
		require.NoError(t, err)
		require.Len(t, res, 6)
		require.Equal(t, DiffItem{"a", DiffSourceLeft}, res[0])
		require.Equal(t, DiffItem{"b", DiffSourceLeft}, res[1])
		require.Equal(t, DiffItem{"c", DiffSourceLeft}, res[2])
		require.Equal(t, DiffItem{"x", DiffSourceRight}, res[3])
		require.Equal(t, DiffItem{"y", DiffSourceRight}, res[4])
		require.Equal(t, DiffItem{"z", DiffSourceRight}, res[5])
	})

	t.Run("Test three left three right elements with common", func(t *testing.T) {
		res, err := Diff([]string{"a", "common", "c"}, []string{"x", "common", "z"})
		require.NoError(t, err)
		require.Len(t, res, 5)
		require.Equal(t, DiffItem{"a", DiffSourceLeft}, res[0])
		require.Equal(t, DiffItem{"x", DiffSourceRight}, res[1])
		require.Equal(t, DiffItem{"common", DiffSourceBoth}, res[2])
		require.Equal(t, DiffItem{"c", DiffSourceLeft}, res[3])
		require.Equal(t, DiffItem{"z", DiffSourceRight}, res[4])
	})

	t.Run("Test swap", func(t *testing.T) {
		res, err := Diff([]string{"common", "a", "b", "common"}, []string{"common", "b", "a", "common"})
		require.NoError(t, err)
		require.Len(t, res, 5)
		require.Equal(t, DiffItem{"common", DiffSourceBoth}, res[0])
		require.Equal(t, DiffItem{"a", DiffSourceLeft}, res[1])
		require.Equal(t, DiffItem{"b", DiffSourceBoth}, res[2])
		require.Equal(t, DiffItem{"a", DiffSourceRight}, res[3])
		require.Equal(t, DiffItem{"common", DiffSourceBoth}, res[4])
	})
}

func TestLineDiff(t *testing.T) {
	someJSONBefore := `      {
         "a": 1,
         "b": null,
         "c": {
           "x": "asdf",
           "z": "qwerty"
         }
       }`
	someJSONAfter := `      {
         "a": 1,
         "c": {
           "x": "asdf",
           "y": [
              "foobar",
              123
           ],
           "z": "edited =)"
         }
       }`
	expectedDiff := `       {
          "a": 1,
-         "b": null,
          "c": {
            "x": "asdf",
-           "z": "qwerty"
+           "y": [
+              "foobar",
+              123
+           ],
+           "z": "edited =)"
          }
        }`
	diffResult, err := LineDiff(someJSONBefore, someJSONAfter)
	require.NoError(t, err)
	require.Equal(t, expectedDiff, diffResult.String())
}
