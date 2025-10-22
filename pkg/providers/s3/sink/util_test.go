package sink

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_provider "github.com/transferia/transferia/pkg/providers/s3"
)

func TestRowFqtn(t *testing.T) {
	tests := []struct {
		name     string
		tableID  abstract.TableID
		expected string
	}{
		{
			name: "with namespace",
			tableID: abstract.TableID{
				Namespace: "test_schema",
				Name:      "test_table",
			},
			expected: "test_schema_test_table",
		},
		{
			name: "without namespace",
			tableID: abstract.TableID{
				Namespace: "",
				Name:      "test_table",
			},
			expected: "test_table",
		},
		{
			name: "empty table name",
			tableID: abstract.TableID{
				Namespace: "test_schema",
				Name:      "",
			},
			expected: "test_schema_",
		},
		{
			name: "both empty",
			tableID: abstract.TableID{
				Namespace: "",
				Name:      "",
			},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rowFqtn(tt.tableID)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestRowPart(t *testing.T) {
	tests := []struct {
		name     string
		row      abstract.ChangeItem
		expected string
	}{
		{
			name: "mirror row",
			row: abstract.ChangeItem{
				ColumnNames: []string{"topic", "partition", "seq_no", "write_time", "data", "meta"},
				ColumnValues: []interface{}{
					"test_topic",
					uint32(1),
					uint64(123),
					"2023-01-01T00:00:00Z",
					"test_data",
					nil,
				},
			},
			expected: "test_topic_1",
		},
		{
			name: "regular row with namespace and partID",
			row: abstract.ChangeItem{
				Schema:       "test_schema",
				Table:        "test_table",
				PartID:       "part123",
				ColumnNames:  []string{"id", "name"},
				ColumnValues: []interface{}{1, "test"},
			},
			expected: "test_schema_test_table_" + hashLongPart("part123", 24),
		},
		{
			name: "regular row without namespace",
			row: abstract.ChangeItem{
				Schema:       "",
				Table:        "test_table",
				PartID:       "part123",
				ColumnNames:  []string{"id", "name"},
				ColumnValues: []interface{}{1, "test"},
			},
			expected: "test_table_" + hashLongPart("part123", 24),
		},
		{
			name: "regular row without partID",
			row: abstract.ChangeItem{
				Schema:       "test_schema",
				Table:        "test_table",
				PartID:       "",
				ColumnNames:  []string{"id", "name"},
				ColumnValues: []interface{}{1, "test"},
			},
			expected: "test_schema_test_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rowPart(tt.row)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestCreateSerializer(t *testing.T) {
	tests := []struct {
		name         string
		outputFormat model.ParsingFormat
		anyAsString  bool
		expectError  bool
	}{
		{
			name:         "raw format",
			outputFormat: model.ParsingFormatRaw,
			anyAsString:  false,
			expectError:  false,
		},
		{
			name:         "json format with anyAsString false",
			outputFormat: model.ParsingFormatJSON,
			anyAsString:  false,
			expectError:  false,
		},
		{
			name:         "json format with anyAsString true",
			outputFormat: model.ParsingFormatJSON,
			anyAsString:  true,
			expectError:  false,
		},
		{
			name:         "csv format",
			outputFormat: model.ParsingFormatCSV,
			anyAsString:  false,
			expectError:  false,
		},
		{
			name:         "parquet format",
			outputFormat: model.ParsingFormatPARQUET,
			anyAsString:  false,
			expectError:  false,
		},
		{
			name:         "unsupported format",
			outputFormat: model.ParsingFormat("UNSUPPORTED"),
			anyAsString:  false,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serializer, err := createSerializer(tt.outputFormat, tt.anyAsString)

			if tt.expectError {
				require.Error(t, err)
				require.Nil(t, serializer)
			} else {
				require.NoError(t, err)
				require.NotNil(t, serializer)
			}
		})
	}
}

func TestHashLongPart(t *testing.T) {
	tests := []struct {
		name       string
		text       string
		maxLen     int
		expectHash bool
	}{
		{
			name:       "short text",
			text:       "short",
			maxLen:     10,
			expectHash: false,
		},
		{
			name:       "exact length text",
			text:       "exactly_ten",
			maxLen:     10,
			expectHash: true,
		},
		{
			name:       "long text",
			text:       "this_is_a_very_long_text_that_should_be_hashed",
			maxLen:     10,
			expectHash: true,
		},
		{
			name:       "empty text",
			text:       "",
			maxLen:     5,
			expectHash: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hashLongPart(tt.text, tt.maxLen)

			if tt.expectHash {
				require.NotEqual(t, tt.text, result)
				require.NotEmpty(t, result)
			} else {
				require.Equal(t, tt.text, result)
			}
		})
	}
}

func TestCreateSnapshotIOHolder(t *testing.T) {
	tests := []struct {
		name           string
		outputEncoding s3_provider.Encoding
		outputFormat   model.ParsingFormat
		anyAsString    bool
		expectError    bool
	}{
		{
			name:           "gzip encoding with raw format",
			outputEncoding: s3_provider.GzipEncoding,
			outputFormat:   model.ParsingFormatRaw,
			anyAsString:    false,
			expectError:    false,
		},
		{
			name:           "no encoding with json format",
			outputEncoding: s3_provider.NoEncoding,
			outputFormat:   model.ParsingFormatJSON,
			anyAsString:    true,
			expectError:    false,
		},
		{
			name:           "gzip encoding with csv format",
			outputEncoding: s3_provider.GzipEncoding,
			outputFormat:   model.ParsingFormatCSV,
			anyAsString:    false,
			expectError:    false,
		},
		{
			name:           "no encoding with parquet format",
			outputEncoding: s3_provider.NoEncoding,
			outputFormat:   model.ParsingFormatPARQUET,
			anyAsString:    false,
			expectError:    false,
		},
		{
			name:           "unsupported format",
			outputEncoding: s3_provider.NoEncoding,
			outputFormat:   model.ParsingFormat("UNSUPPORTED"),
			anyAsString:    false,
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			holder, err := createSnapshotIOHolder(tt.outputEncoding, tt.outputFormat, tt.anyAsString)

			if tt.expectError {
				require.Error(t, err)
				require.Nil(t, holder)
			} else {
				require.NoError(t, err)
				require.NotNil(t, holder)
				require.NotNil(t, holder.uploadDone)
				require.NotNil(t, holder.snapshot)
				require.NotNil(t, holder.serializer)
			}
		})
	}
}
