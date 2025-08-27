package regexreplace

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestTransformer_Apply(t *testing.T) {
	t.Run("no_settings", func(t *testing.T) {
		transformer := new(Transformer)

		table := abstract.TableID{
			Namespace: "db",
			Name:      "table",
		}

		schema := abstract.NewTableSchema(abstract.TableColumns{
			abstract.MakeTypedColSchema("column1", string(schema.TypeString), true),
			abstract.MakeTypedColSchema("column2", string(schema.TypeInt64), false),
			abstract.MakeTypedColSchema("column3", string(schema.TypeInt32), false),
			abstract.MakeTypedColSchema("column4", string(schema.TypeBoolean), false),
		})

		require.True(t, transformer.Suitable(table, schema))

		items := []abstract.ChangeItem{
			{
				Kind:         "Insert",
				Schema:       "db",
				Table:        "table1",
				ColumnNames:  []string{"column1", "column2", "column3", "column4"},
				ColumnValues: []any{"value1", int64(123), int32(1234), true},
				TableSchema:  schema,
			},
		}

		expected := abstract.TransformerResult{
			Transformed: items,
		}

		result := transformer.Apply(items)

		assert.Equal(t, expected, result)
	})

	t.Run("replaced", func(t *testing.T) {
		transformer := &Transformer{
			MatchRegex:  regexp.MustCompile(`[_@#&]`),
			ReplaceRule: `-`,
		}

		table := abstract.TableID{
			Namespace: "db",
			Name:      "table",
		}

		schema := abstract.NewTableSchema(abstract.TableColumns{
			abstract.MakeTypedColSchema("column1", string(schema.TypeString), true),
			abstract.MakeTypedColSchema("column2", string(schema.TypeInt64), false),
			abstract.MakeTypedColSchema("column3", string(schema.TypeInt32), false),
			abstract.MakeTypedColSchema("column4", string(schema.TypeBytes), true),
			abstract.MakeTypedColSchema("column5", string(schema.TypeBoolean), false),
		})

		require.True(t, transformer.Suitable(table, schema))

		items := []abstract.ChangeItem{
			{
				Kind:         "Insert",
				Schema:       "db",
				Table:        "table1",
				ColumnNames:  []string{"column1", "column2", "column3", "column4", "column5"},
				ColumnValues: []any{"value_1", int64(123), int32(1234), []byte("value@2"), true},
				TableSchema:  schema,
			},
		}

		expected := abstract.TransformerResult{
			Transformed: []abstract.ChangeItem{
				{
					Kind:         "Insert",
					Schema:       "db",
					Table:        "table1",
					ColumnNames:  []string{"column1", "column2", "column3", "column4", "column5"},
					ColumnValues: []any{"value-1", int64(123), int32(1234), []byte("value-2"), true},
					TableSchema:  schema,
				},
			},
		}

		result := transformer.Apply(items)

		assert.Equal(t, expected, result)
	})

	t.Run("table_filter", func(t *testing.T) {
		tf, err := filter.NewFilter(nil, []string{`bad_.*`})
		require.NoError(t, err)

		transformer := &Transformer{
			MatchRegex:  regexp.MustCompile(`[_@#&]`),
			ReplaceRule: `-`,
			Tables:      tf,
		}

		table := abstract.TableID{
			Namespace: "db",
			Name:      "bad_table",
		}

		schema := abstract.NewTableSchema(abstract.TableColumns{
			abstract.MakeTypedColSchema("column1", string(schema.TypeString), true),
			abstract.MakeTypedColSchema("column2", string(schema.TypeInt64), false),
			abstract.MakeTypedColSchema("column3", string(schema.TypeInt32), false),
			abstract.MakeTypedColSchema("column4", string(schema.TypeBytes), true),
			abstract.MakeTypedColSchema("column5", string(schema.TypeBoolean), false),
		})

		assert.False(t, transformer.Suitable(table, schema))
	})

	t.Run("column_filter", func(t *testing.T) {
		cf, err := filter.NewFilter(nil, []string{`.*_private`})
		require.NoError(t, err)

		transformer := &Transformer{
			MatchRegex:  regexp.MustCompile(`[_@#&]`),
			ReplaceRule: `-`,
			Columns:     cf,
		}

		table := abstract.TableID{
			Namespace: "db",
			Name:      "table",
		}

		schema := abstract.NewTableSchema(abstract.TableColumns{
			abstract.MakeTypedColSchema("column1", string(schema.TypeString), true),
			abstract.MakeTypedColSchema("column2", string(schema.TypeInt64), false),
			abstract.MakeTypedColSchema("column3", string(schema.TypeInt32), false),
			abstract.MakeTypedColSchema("column4_private", string(schema.TypeBytes), true),
			abstract.MakeTypedColSchema("column5", string(schema.TypeBoolean), false),
		})

		require.True(t, transformer.Suitable(table, schema))

		items := []abstract.ChangeItem{
			{
				Kind:         "Insert",
				Schema:       "db",
				Table:        "table1",
				ColumnNames:  []string{"column1", "column2", "column3", "column4_private", "column5"},
				ColumnValues: []any{"value_1", int64(123), int32(1234), []byte("value@2"), true},
				TableSchema:  schema,
			},
		}

		expected := abstract.TransformerResult{
			Transformed: []abstract.ChangeItem{
				{
					Kind:         "Insert",
					Schema:       "db",
					Table:        "table1",
					ColumnNames:  []string{"column1", "column2", "column3", "column4_private", "column5"},
					ColumnValues: []any{"value-1", int64(123), int32(1234), []byte("value@2"), true},
					TableSchema:  schema,
				},
			},
		}

		result := transformer.Apply(items)

		assert.Equal(t, expected, result)
	})
}

func TestReplace(t *testing.T) {
	matchRegex := regexp.MustCompile(`\d+`)
	replaceRule := "NUM"

	tests := []struct {
		name     string
		value    any
		typ      string
		expected any
	}{
		{
			name:     "string with digits",
			value:    "test123",
			typ:      schema.TypeString.String(),
			expected: "testNUM",
		},
		{
			name:     "string without digits",
			value:    "test",
			typ:      schema.TypeString.String(),
			expected: "test",
		},
		{
			name:     "empty string",
			value:    "",
			typ:      schema.TypeString.String(),
			expected: "",
		},
		{
			name:     "bytes with digits",
			value:    []byte("test123"),
			typ:      schema.TypeBytes.String(),
			expected: []byte("testNUM"),
		},
		{
			name:     "bytes without digits",
			value:    []byte("test"),
			typ:      schema.TypeBytes.String(),
			expected: []byte("test"),
		},
		{
			name:     "empty bytes",
			value:    []byte(""),
			typ:      schema.TypeBytes.String(),
			expected: []byte(nil),
		},
		{
			name:     "non-string type",
			value:    123,
			typ:      "int",
			expected: 123,
		},
		{
			name:     "nil value",
			value:    nil,
			typ:      schema.TypeString.String(),
			expected: nil,
		},
		{
			name:     "wrong type assertion",
			value:    123,
			typ:      schema.TypeString.String(),
			expected: 123,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := replace(tt.value, tt.typ, matchRegex, replaceRule)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestReplaceMultipleMatches(t *testing.T) {
	matchRegex := regexp.MustCompile(`a`)
	replaceRule := "b"

	tests := []struct {
		name     string
		value    any
		typ      string
		expected any
	}{
		{
			name:     "string multiple matches",
			value:    "banana",
			typ:      schema.TypeString.String(),
			expected: "bbnbnb",
		},
		{
			name:     "bytes multiple matches",
			value:    []byte("banana"),
			typ:      schema.TypeBytes.String(),
			expected: []byte("bbnbnb"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := replace(tt.value, tt.typ, matchRegex, replaceRule)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestReplaceComplexRegex(t *testing.T) {
	matchRegex := regexp.MustCompile(`(\w+)\s(\w+)`)
	replaceRule := "$2, $1"

	tests := []struct {
		name     string
		value    any
		typ      string
		expected any
	}{
		{
			name:     "string with groups",
			value:    "John Doe",
			typ:      schema.TypeString.String(),
			expected: "Doe, John",
		},
		{
			name:     "bytes with groups",
			value:    []byte("John Doe"),
			typ:      schema.TypeBytes.String(),
			expected: []byte("Doe, John"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := replace(tt.value, tt.typ, matchRegex, replaceRule)
			assert.Equal(t, tt.expected, result)
		})
	}
}
