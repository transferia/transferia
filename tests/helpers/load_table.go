package helpers

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
)

func compare(l, r interface{}) (bool, bool) { // equal, less
	switch ll := l.(type) {
	case int8:
		rr := r.(int8)
		return ll == rr, ll < rr
	case int16:
		rr := r.(int16)
		return ll == rr, ll < rr
	case int32:
		rr := r.(int32)
		return ll == rr, ll < rr
	case int64:
		rr := r.(int64)
		return ll == rr, ll < rr
	case uint8:
		rr := r.(uint8)
		return ll == rr, ll < rr
	case uint16:
		rr := r.(uint16)
		return ll == rr, ll < rr
	case uint32:
		rr := r.(uint32)
		return ll == rr, ll < rr
	case int:
		rr := r.(int)
		return ll == rr, ll < rr
	case uint:
		rr := r.(uint)
		return ll == rr, ll < rr
	case uint64:
		rr := r.(uint64)
		return ll == rr, ll < rr
	case float32:
		rr := r.(float32)
		return ll == rr, ll < rr
	case float64:
		rr := r.(float64)
		return ll == rr, ll < rr
	case string:
		rr := r.(string)
		return ll == rr, ll < rr
	case []byte:
		panic("unsupported")
	case time.Time:
		panic("unsupported")
	default:
		panic("unsupported")
	}
}

func sortChangeItems(t *testing.T, data []abstract.ChangeItem) []abstract.ChangeItem {
	if len(data) == 0 {
		return data
	}

	// assumption: all changeItems has the same schema

	firstSchema := data[0].TableSchema
	for i := range data {
		require.Equal(t, firstSchema, data[i].TableSchema)
	}

	// sort

	sort.Slice(data, func(i, j int) bool {
		cols := firstSchema.Columns()

		lVal := data[i].AsMap()
		rVal := data[j].AsMap()

		// check pkeys
		for _, el := range cols {
			if el.PrimaryKey {
				colName := el.ColumnName
				equal, less := compare(lVal[colName], rVal[colName])
				if equal {
					continue
				}
				return less
			}
		}

		panic("unsupported")
	})

	return data
}

func LoadTable(t *testing.T, storage abstract.Storage, table abstract.TableDescription) []abstract.ChangeItem {
	data := make([]abstract.ChangeItem, 0)
	require.NoError(t, storage.LoadTable(context.Background(), table, func(input []abstract.ChangeItem) error {
		for _, row := range input {
			if row.Kind == abstract.InsertKind {
				data = append(data, row)
			}
		}
		return nil
	}))

	return sortChangeItems(t, data)
}
