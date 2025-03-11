package generic

import (
	_ "embed"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/internal/metrics"
	"github.com/transferria/transferria/library/go/test/canon"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/parsers"
	"github.com/transferria/transferria/pkg/stats"
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

				result := parser.Do(msg, abstract.Partition{Cluster: "", Partition: 0, Topic: ""})
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

				result := parserWithNumbers.Do(msg, abstract.Partition{Cluster: "", Partition: 0, Topic: ""})
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

		result := parser.Do(msg, abstract.Partition{Cluster: "", Partition: 0, Topic: ""})
		require.Len(t, result, 1)
		abstract.Dump(result)
		for index := range result {
			result[index].CommitTime = 0
		}
		canonResult.UseNumbersFalse = append(canonResult.UseNumbersFalse, result[0])

		resultWithNumbers := parserWithNumbers.Do(msg, abstract.Partition{Cluster: "", Partition: 0, Topic: ""})
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

		result := parserJSON.Do(msg, abstract.Partition{Cluster: "", Partition: 0, Topic: ""})
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

		result := parserJSON.Do(msg, abstract.Partition{Cluster: "", Partition: 0, Topic: ""})
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

		result := parserJSON.Do(msg, abstract.Partition{Cluster: "", Partition: 0, Topic: ""})
		require.Len(t, result, 1)
		abstract.Dump(result)
		for index := range result {
			result[index].CommitTime = 0
			canonResult = append(canonResult, result[index])
		}
	}
	canon.SaveJSON(t, canonResult)
}
