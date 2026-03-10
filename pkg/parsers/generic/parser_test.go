package generic

import (
	_ "embed"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/internal/metrics"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/yt/go/schema"
)

//go:embed test_data/parser_numbers_test.jsonl
var parserTestNumbers []byte

//go:embed test_data/parser_unescape_test.jsonl
var parserTestJSONUnescape []byte

//go:embed test_data/parser_unescape_test.tskv
var parserTestTSKVUnescape []byte

//go:embed test_data/parse_base64_packed.jsonl
var parserBase64Encoded []byte

func makePersqueueReadMessage(i int, rawLine string) parsers.Message {

	return parsers.Message{
		Offset:     uint64(i),
		SeqNo:      0,
		Key:        []byte("test_source_id"),
		CreateTime: time.Now(),
		WriteTime:  time.Now(),
		Value:      []byte(rawLine),
		Headers:    nil,
	}
}

func BenchmarkNumberType(b *testing.B) {
	rawLines := strings.Split(string(parserTestNumbers), "\n")

	fields := []abstract.ColSchema{
		{
			ColumnName: "id",
			DataType:   schema.TypeInt8.String(),
		},
		{
			ColumnName: "number_field",
			DataType:   schema.TypeInt64.String(),
		},
		{
			ColumnName: "float_field",
			DataType:   schema.TypeFloat64.String(),
		},
		{
			ColumnName: "obj_field",
			DataType:   schema.TypeAny.String(),
		},
		{
			ColumnName: "array_field",
			DataType:   schema.TypeAny.String(),
		},
	}

	parserConfig := &GenericParserConfig{
		Format:             "json",
		SchemaResourceName: "",
		Fields:             fields,
		AuxOpts: AuxParserOpts{
			Topic: "my_topic_name",
		},
	}
	parser := NewGenericParser(parserConfig, fields, logger.Log, stats.NewSourceStats(metrics.NewRegistry().WithTags(map[string]string{"id": "TestParserNumberTypes"})))

	parserConfigUseNumbers := &GenericParserConfig{
		Format:             "json",
		SchemaResourceName: "",
		Fields:             fields,
		AuxOpts: AuxParserOpts{
			Topic:           "my_topic_name",
			UseNumbersInAny: true,
		},
	}
	parserWithNumbers := NewGenericParser(parserConfigUseNumbers, fields, logger.Log, stats.NewSourceStats(metrics.NewRegistry().WithTags(map[string]string{"id": "TestParserNumberTypes"})))

	b.Run("no-num", func(b *testing.B) {
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			size := int64(0)
			for i, line := range rawLines {
				if line == "" {
					continue
				}
				msg := makePersqueueReadMessage(i, line)

				result := parser.Do(msg, abstract.Partition{Partition: 0, Topic: ""})
				require.True(b, len(result) > 0)
				size += int64(len(msg.Value))
			}
			b.SetBytes(size)
		}
		b.ReportAllocs()
	})
	b.Run("num", func(b *testing.B) {
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			size := int64(0)
			for i, line := range rawLines {
				if line == "" {
					continue
				}
				msg := makePersqueueReadMessage(i, line)

				result := parserWithNumbers.Do(msg, abstract.Partition{Partition: 0, Topic: ""})
				require.True(b, len(result) > 0)
				size += int64(len(msg.Value))
			}
			b.SetBytes(size)
		}
		b.ReportAllocs()
	})
}

func TestParserNumberTypes(t *testing.T) {
	rawLines := strings.Split(string(parserTestNumbers), "\n")

	fields := []abstract.ColSchema{
		{
			ColumnName: "id",
			DataType:   schema.TypeInt8.String(),
		},
		{
			ColumnName: "number_field",
			DataType:   schema.TypeInt64.String(),
		},
		{
			ColumnName: "float_field",
			DataType:   schema.TypeFloat64.String(),
		},
		{
			ColumnName: "obj_field",
			DataType:   schema.TypeAny.String(),
		},
		{
			ColumnName: "array_field",
			DataType:   schema.TypeAny.String(),
		},
	}

	parserConfig := &GenericParserConfig{
		Format:             "json",
		SchemaResourceName: "",
		Fields:             fields,
		AuxOpts: AuxParserOpts{
			Topic:                  "my_topic_name",
			AddDedupeKeys:          false,
			MarkDedupeKeysAsSystem: false,
			AddSystemColumns:       false,
			AddTopicColumn:         false,
			AddRest:                false,
			TimeField:              nil,
			InferTimeZone:          false,
			NullKeysAllowed:        false,
			DropUnparsed:           false,
			MaskSecrets:            false,
			IgnoreColumnPaths:      false,
			TableSplitter:          nil,
			Sniff:                  false,
			UseNumbersInAny:        false,
		},
	}
	parser := NewGenericParser(parserConfig, fields, logger.Log, stats.NewSourceStats(metrics.NewRegistry().WithTags(map[string]string{"id": "TestParserNumberTypes"})))

	parserConfigUseNumbers := &GenericParserConfig{
		Format:             "json",
		SchemaResourceName: "",
		Fields:             fields,
		AuxOpts: AuxParserOpts{
			Topic:                  "my_topic_name",
			AddDedupeKeys:          false,
			MarkDedupeKeysAsSystem: false,
			AddSystemColumns:       false,
			AddTopicColumn:         false,
			AddRest:                false,
			TimeField:              nil,
			InferTimeZone:          false,
			NullKeysAllowed:        false,
			DropUnparsed:           false,
			MaskSecrets:            false,
			IgnoreColumnPaths:      false,
			TableSplitter:          nil,
			Sniff:                  false,
			UseNumbersInAny:        true,
		},
	}
	parserWithNumbers := NewGenericParser(parserConfigUseNumbers, fields, logger.Log, stats.NewSourceStats(metrics.NewRegistry().WithTags(map[string]string{"id": "TestParserNumberTypes"})))

	canonResult := struct {
		UseNumbersTrue  []interface{}
		UseNumbersFalse []interface{}
	}{}

	for i, line := range rawLines {
		if line == "" {
			continue
		}
		msg := makePersqueueReadMessage(i, line)

		result := parser.Do(msg, abstract.Partition{Partition: 0, Topic: ""})
		require.Len(t, result, 1)
		abstract.Dump(result)
		for index := range result {
			result[index].CommitTime = 0
		}
		canonResult.UseNumbersFalse = append(canonResult.UseNumbersFalse, result[0])

		resultWithNumbers := parserWithNumbers.Do(msg, abstract.Partition{Partition: 0, Topic: ""})
		require.Len(t, resultWithNumbers, 1)
		abstract.Dump(resultWithNumbers)
		for index := range resultWithNumbers {
			resultWithNumbers[index].CommitTime = 0
		}
		canonResult.UseNumbersTrue = append(canonResult.UseNumbersTrue, resultWithNumbers[0])
	}
	canon.SaveJSON(t, canonResult)
}

func TestBase64Unpack(t *testing.T) {
	fields := []abstract.ColSchema{
		{
			ColumnName: "id",
			DataType:   schema.TypeInt8.String(),
		},
		{
			ColumnName: "stringVal",
			DataType:   schema.TypeString.String(),
		},
		{
			ColumnName: "bytesVal",
			DataType:   schema.TypeBytes.String(),
		},
	}
	rawLines := strings.Split(string(parserBase64Encoded), "\n")
	parserConfigJSON := &GenericParserConfig{
		Format:             "json",
		SchemaResourceName: "",
		Fields:             fields,
		AuxOpts: AuxParserOpts{
			Topic:             "my_topic_name",
			UnpackBytesBase64: true,
		},
	}
	parserJSON := NewGenericParser(parserConfigJSON, fields, logger.Log, stats.NewSourceStats(metrics.NewRegistry().WithTags(map[string]string{"id": "TestParserNumberTypes"})))

	var canonResult []interface{}
	for i, line := range rawLines {
		if line == "" {
			continue
		}
		msg := makePersqueueReadMessage(i, line)

		result := parserJSON.Do(msg, abstract.Partition{Partition: 0, Topic: ""})
		require.Len(t, result, 1)
		abstract.Dump(result)
		for index := range result {
			result[index].CommitTime = 0
			canonResult = append(canonResult, result[index])
		}
	}
	canon.SaveJSON(t, canonResult)
}

func TestUnescapeJSON(t *testing.T) {
	fields := []abstract.ColSchema{
		{
			ColumnName: "id",
			DataType:   schema.TypeInt8.String(),
		},
		{
			ColumnName: "stringVal",
			DataType:   schema.TypeString.String(),
		},
		{
			ColumnName: "escapedStringVal",
			DataType:   schema.TypeString.String(),
		},
	}
	rawLines := strings.Split(string(parserTestJSONUnescape), "\n")
	parserConfigJSON := &GenericParserConfig{
		Format:             "json",
		SchemaResourceName: "",
		Fields:             fields,
		AuxOpts: AuxParserOpts{
			Topic:                "my_topic_name",
			UnescapeStringValues: true,
		},
	}
	parserJSON := NewGenericParser(parserConfigJSON, fields, logger.Log, stats.NewSourceStats(metrics.NewRegistry().WithTags(map[string]string{"id": "TestParserNumberTypes"})))

	var canonResult []interface{}
	for i, line := range rawLines {
		if line == "" {
			continue
		}
		msg := makePersqueueReadMessage(i, line)

		result := parserJSON.Do(msg, abstract.Partition{Partition: 0, Topic: ""})
		require.Len(t, result, 1)
		abstract.Dump(result)
		for index := range result {
			result[index].CommitTime = 0
			canonResult = append(canonResult, result[index])
		}
	}
	canon.SaveJSON(t, canonResult)
}

func TestUnescapeTSKV(t *testing.T) {
	rawLines := strings.Split(string(parserTestTSKVUnescape), "\n")
	fields := []abstract.ColSchema{
		{
			ColumnName: "id",
			DataType:   schema.TypeInt8.String(),
		},
		{
			ColumnName: "message",
			DataType:   schema.TypeBytes.String(),
		},
	}

	parserConfigTSKV := &GenericParserConfig{
		Format:             "tskv",
		SchemaResourceName: "",
		Fields:             fields,
		AuxOpts: AuxParserOpts{
			Topic:                "my_topic_name",
			UnescapeStringValues: true,
		},
	}
	parserJSON := NewGenericParser(parserConfigTSKV, fields, logger.Log, stats.NewSourceStats(metrics.NewRegistry().WithTags(map[string]string{"id": "TestParserNumberTypes"})))

	var canonResult []interface{}
	for i, line := range rawLines {
		if line == "" {
			continue
		}
		msg := makePersqueueReadMessage(i, line)

		result := parserJSON.Do(msg, abstract.Partition{Partition: 0, Topic: ""})
		require.Len(t, result, 1)
		abstract.Dump(result)
		for index := range result {
			result[index].CommitTime = 0
			canonResult = append(canonResult, result[index])
		}
	}
	canon.SaveJSON(t, canonResult)
}

// TestTimezoneRFC3339Parsing tests that both RFC3339 formats (Z suffix and +XX:YY offset) are parsed correctly
// This addresses the issue where "2026-02-03T12:51:05.000225908Z" was incorrectly parsed in local timezone (MSK)
// while "2026-02-04T07:34:12.001036741+00:00" was parsed correctly in UTC
func TestTimezoneRFC3339Parsing(t *testing.T) {
	fields := []abstract.ColSchema{
		{
			ColumnName: "id",
			DataType:   schema.TypeInt8.String(),
		},
		{
			ColumnName: "timestamp_z",
			DataType:   "datetime",
		},
		{
			ColumnName: "timestamp_offset",
			DataType:   "datetime",
		},
	}

	testCases := []struct {
		name           string
		jsonLine       string
		timezone       string
		expectedTimeZ  time.Time // Expected time for timestamp_z field
		expectedOffset time.Time // Expected time for timestamp_offset field
	}{
		{
			name:           "RFC3339 with Z suffix and +00:00 offset - UTC timezone specified",
			jsonLine:       `{"id":1,"timestamp_z":"2026-02-03T12:51:05.000225908Z","timestamp_offset":"2026-02-04T07:34:12.001036741+00:00"}`,
			timezone:       "UTC",
			expectedTimeZ:  time.Date(2026, 2, 3, 12, 51, 5, 225908, time.UTC),
			expectedOffset: time.Date(2026, 2, 4, 7, 34, 12, 1036741, time.UTC),
		},
		{
			name:           "RFC3339 with Z suffix and +00:00 offset - Moscow timezone specified",
			jsonLine:       `{"id":1,"timestamp_z":"2026-02-03T12:51:05.000225908Z","timestamp_offset":"2026-02-04T07:34:12.001036741+00:00"}`,
			timezone:       "UTC",
			expectedTimeZ:  time.Date(2026, 2, 3, 12, 51, 5, 225908, time.UTC),
			expectedOffset: time.Date(2026, 2, 4, 7, 34, 12, 1036741, time.UTC),
		},
		{
			name:           "RFC3339 with Z suffix without nanoseconds - UTC timezone specified",
			jsonLine:       `{"id":2,"timestamp_z":"2026-02-03T12:51:05Z","timestamp_offset":"2026-02-04T07:34:12+00:00"}`,
			timezone:       "UTC",
			expectedTimeZ:  time.Date(2026, 2, 3, 12, 51, 5, 0, time.UTC),
			expectedOffset: time.Date(2026, 2, 4, 7, 34, 12, 0, time.UTC),
		},
		{
			name:           "RFC3339 with +03:00 offset (Moscow time) - UTC timezone specified",
			jsonLine:       `{"id":3,"timestamp_z":"2026-02-03T15:51:05+03:00","timestamp_offset":"2026-02-04T10:34:12+03:00"}`,
			timezone:       "UTC",
			expectedTimeZ:  time.Date(2026, 2, 3, 12, 51, 5, 0, time.UTC), // Should be converted to UTC
			expectedOffset: time.Date(2026, 2, 4, 7, 34, 12, 0, time.UTC), // Should be converted to UTC
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parserConfig := &GenericParserConfig{
				Format:             "json",
				SchemaResourceName: "",
				Fields:             fields,
				AuxOpts: AuxParserOpts{
					Topic:    "test_topic",
					Timezone: tc.timezone,
				},
			}
			parser := NewGenericParser(parserConfig, fields, logger.Log, stats.NewSourceStats(metrics.NewRegistry().WithTags(map[string]string{"id": "TestTimezoneRFC3339Parsing"})))

			msg := makePersqueueReadMessage(1, tc.jsonLine)
			result := parser.Do(msg, abstract.Partition{Partition: 0, Topic: ""})

			require.Len(t, result, 1, "Expected exactly one parsed result")
			require.Equal(t, abstract.InsertKind, result[0].Kind, "Expected InsertKind")

			// Check that both timestamps are parsed correctly
			timestampZ, ok := result[0].ColumnValues[1].(time.Time)
			require.True(t, ok, "timestamp_z should be time.Time")
			timestampOffset, ok := result[0].ColumnValues[2].(time.Time)
			require.True(t, ok, "timestamp_offset should be time.Time")

			// Verify timestamps are in UTC and match expected values
			require.Equal(t, tc.expectedTimeZ.UTC(), timestampZ.UTC(), "timestamp_z should match expected UTC time")
			require.Equal(t, tc.expectedOffset.UTC(), timestampOffset.UTC(), "timestamp_offset should match expected UTC time")
		})
	}
}
