package serializer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestJSONSerializerUnsupportedKind(t *testing.T) {
	item := abstract.ChangeItem{
		Kind: abstract.DeleteKind,
	}

	s := NewJSONSerializer(
		&JSONSerializerConfig{
			UnsupportedItemKinds: nil,
		},
	)

	_, err := s.Serialize(&item)
	require.NoError(t, err)

	unsupported := map[abstract.Kind]bool{
		item.Kind: true,
	}

	s = NewJSONSerializer(
		&JSONSerializerConfig{
			UnsupportedItemKinds: unsupported,
		},
	)

	_, err = s.Serialize(&item)
	require.Error(t, err)

	delete(unsupported, item.Kind)

	_, err = s.Serialize(&item)
	require.NoError(t, err)

	// check non-row-item

	item.Kind = abstract.InitTableLoad
	serializedBuf, err := s.Serialize(&item)
	require.NoError(t, err)
	require.Nil(t, serializedBuf)
}

func TestJSONSerializerComplexAsStr(t *testing.T) {
	item := abstract.ChangeItem{
		Kind: abstract.InsertKind,
		TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
			{DataType: string(schema.TypeInt16)},
			{DataType: string(schema.TypeAny)},
			{DataType: string(schema.TypeAny)},
			{DataType: string(schema.TypeAny)},
		}),
		ColumnNames: []string{
			"id",
			"jsonObject",
			"jsonArray",
			"nil",
		},
		ColumnValues: []any{
			1,
			map[string]string{"key": "value"},
			[]int{1, 2, 3},
			nil,
		},
	}
	tests := []struct {
		name           string
		anyAsString    bool
		expectedObject string
		expectedArray  string
	}{
		{
			name:           "Complex to string",
			anyAsString:    true,
			expectedObject: "\"{\\\"key\\\":\\\"value\\\"}\"",
			expectedArray:  "\"[1,2,3]\"",
		},
		{
			name:           "Complex as is",
			anyAsString:    false,
			expectedObject: "{\"key\":\"value\"}",
			expectedArray:  "[1,2,3]",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := NewJSONSerializer(
				&JSONSerializerConfig{
					AnyAsString: tc.anyAsString,
				},
			)

			serialized, err := s.Serialize(&item)
			require.NoError(t, err)

			serialzedStr := string(serialized)
			require.Contains(t, serialzedStr, "\"id\":1")
			require.Contains(t, serialzedStr, fmt.Sprintf("\"jsonObject\":%v", tc.expectedObject))
			require.Contains(t, serialzedStr, fmt.Sprintf("\"jsonArray\":%v", tc.expectedArray))
			require.Contains(t, serialzedStr, "\"nil\":null")
		})
	}
}
