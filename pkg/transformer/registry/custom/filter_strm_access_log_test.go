package custom

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
)

func TestFilterStrmAccessLog(t *testing.T) {
	filter := NewFilterStrmAccessLog(logger.Log)

	t.Run("not insert", func(t *testing.T) {
		result := filter.Apply([]abstract.ChangeItem{abstract.MakeSynchronizeEvent()})
		require.Len(t, result.Transformed, 1)
	})

	t.Run("absent unistat_tier", func(t *testing.T) {
		result := filter.Apply([]abstract.ChangeItem{{
			Kind:         abstract.InsertKind,
			ColumnNames:  []string{"traffic_type"},
			ColumnValues: []interface{}{"users"},
		}})
		require.Len(t, result.Transformed, 0)
	})
	t.Run("absent traffic_type", func(t *testing.T) {
		result := filter.Apply([]abstract.ChangeItem{{
			Kind:         abstract.InsertKind,
			ColumnNames:  []string{"unistat_tier"},
			ColumnValues: []interface{}{"zen-vod"},
		}})
		require.Len(t, result.Transformed, 0)
	})

	t.Run("some field is nil", func(t *testing.T) {
		result := filter.Apply([]abstract.ChangeItem{{
			Kind:         abstract.InsertKind,
			ColumnNames:  []string{"traffic_type", "unistat_tier"},
			ColumnValues: []interface{}{"users", nil},
		}})
		require.Len(t, result.Transformed, 0)
	})

	t.Run("some field is not string", func(t *testing.T) {
		result := filter.Apply([]abstract.ChangeItem{{
			Kind:         abstract.InsertKind,
			ColumnNames:  []string{"traffic_type", "unistat_tier"},
			ColumnValues: []interface{}{"users", 1},
		}})
		require.Len(t, result.Transformed, 0)
	})

	t.Run("works - filter out", func(t *testing.T) {
		result := filter.Apply([]abstract.ChangeItem{{
			Kind:         abstract.InsertKind,
			ColumnNames:  []string{"traffic_type", "unistat_tier"},
			ColumnValues: []interface{}{"users", "zen-vod2"},
		}})
		require.Len(t, result.Transformed, 0)
	})
	t.Run("works - not filter out", func(t *testing.T) {
		result := filter.Apply([]abstract.ChangeItem{{
			Kind:         abstract.InsertKind,
			ColumnNames:  []string{"traffic_type", "unistat_tier"},
			ColumnValues: []interface{}{"users", "zen-vod"},
		}})
		require.Len(t, result.Transformed, 1)
	})
}
