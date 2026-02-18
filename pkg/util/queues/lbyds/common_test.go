package lbyds

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/generic"
	jsonparser "github.com/transferia/transferia/pkg/parsers/registry/json"
	"github.com/transferia/transferia/pkg/stats"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestParse(t *testing.T) {
	sourceMetrics := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))

	parserConfigStruct := &jsonparser.ParserConfigJSONLb{
		Fields: []abstract.ColSchema{
			{ColumnName: "msg", DataType: ytschema.TypeBytes.String()},
		},
		AddRest: false,
	}
	parserConfigMap, err := parsers.ParserConfigStructToMap(parserConfigStruct)
	require.NoError(t, err)
	parser, err := parsers.NewParserFromMap(parserConfigMap, false, logger.Log, sourceMetrics)
	require.NoError(t, err)

	testTopic := "/directory/test-topic"
	testData := []parsers.MessageBatch{
		{
			Topic:     testTopic,
			Partition: 0,
			Messages:  []parsers.Message{{Offset: 0, Key: []byte("some_key"), Value: []byte("{\"msg\":\"some_value\"}")}},
		},
		{
			Topic:     testTopic,
			Partition: 1,
			Messages:  []parsers.Message{{Offset: 1, Key: []byte("some_key"), Value: []byte("{\"msg\":\"some_value\"}")}},
		},
	}

	t.Run("WithTransformOnly", func(t *testing.T) {
		transformedData := "{\"msg\":\"transformed_value\"}"
		res := Parse(testData, nil, sourceMetrics, logger.Log, func(items []abstract.ChangeItem) []abstract.ChangeItem {
			for _, item := range items {
				item.ColumnValues[abstract.RawDataColsIDX[abstract.RawMessageData]] = transformedData
			}
			return items
		}, false)

		require.Len(t, res, len(testData))
		for _, item := range res {
			require.Equal(t, "test-topic", item.Table)
			require.Equal(t, transformedData, item.ColumnValues[abstract.RawDataColsIDX[abstract.RawMessageData]])
		}
	})

	t.Run("WithTransformAndParser", func(t *testing.T) {
		transformedData := "{\"msg\":\"transformed_value\"}"
		res := Parse(testData, parser, sourceMetrics, logger.Log, func(items []abstract.ChangeItem) []abstract.ChangeItem {
			for _, item := range items {
				item.ColumnValues[abstract.RawDataColsIDX[abstract.RawMessageData]] = transformedData
			}
			return items
		}, false)

		require.Len(t, res, len(testData))
		for idx, item := range res {
			require.Equal(t, "msg", item.ColumnNames[0])
			require.Equal(t, "transformed_value", item.ColumnValues[0])

			// test data contains only two partitions 0 and 1
			var expectedPartition = uint32(idx)
			partition := abstract.Partition{
				Cluster:   "",
				Topic:     "test-topic",
				Partition: expectedPartition,
			}
			require.Equal(t, "test-topic", item.Table)
			require.Equal(t, partition.String(), item.PartID)
			require.Equal(t, partition.String(), item.ColumnValues[generic.PartitionIDX(item.ColumnNames)])
		}
	})

	t.Run("ParserOnly", func(t *testing.T) {
		res := Parse(testData, parser, sourceMetrics, logger.Log, nil, false)

		require.Len(t, res, len(testData))
		for idx, item := range res {
			require.Equal(t, "msg", item.ColumnNames[0])
			require.Equal(t, "some_value", item.ColumnValues[0])

			// test data contains only two partitions 0 and 1
			var expectedPartition = uint32(idx)
			partition := abstract.Partition{
				Cluster:   "",
				Topic:     "test-topic",
				Partition: expectedPartition,
			}
			require.Equal(t, "test-topic", item.Table)
			require.Equal(t, partition.String(), item.PartID)
			require.Equal(t, partition.String(), item.ColumnValues[generic.PartitionIDX(item.ColumnNames)])
		}
	})

	t.Run("ParserOnlyWithUseFullTopicName", func(t *testing.T) {
		res := Parse(testData, parser, sourceMetrics, logger.Log, nil, true)

		require.Len(t, res, len(testData))
		for idx, item := range res {
			require.Equal(t, "msg", item.ColumnNames[0])
			require.Equal(t, "some_value", item.ColumnValues[0])

			// test data contains only two partitions 0 and 1
			var expectedPartition = uint32(idx)
			partition := abstract.Partition{
				Cluster:   "",
				Topic:     "/directory/test-topic",
				Partition: expectedPartition,
			}
			require.Equal(t, "_directory_test-topic", item.Table)
			require.Equal(t, partition.String(), item.PartID)
			require.Equal(t, partition.String(), item.ColumnValues[generic.PartitionIDX(item.ColumnNames)])
		}
	})
}

func TestCombineBatches(t *testing.T) {
	t.Run("EmptyInput", func(t *testing.T) {
		res := combineBatches([]parsers.MessageBatch{})

		require.Len(t, res, 0)
	})

	t.Run("NoCombinations", func(t *testing.T) {
		res := combineBatches([]parsers.MessageBatch{
			{Topic: "/directory/test-topic", Partition: 0, Messages: []parsers.Message{{Offset: 0}}},
			{Topic: "/directory/test-topic", Partition: 1, Messages: []parsers.Message{{Offset: 0}}},
		})

		require.Len(t, res, 2)
		require.Len(t, res[0].Messages, 1)
		require.Len(t, res[1].Messages, 1)
		require.Equal(t, uint64(0), res[0].Messages[0].Offset)
		require.Equal(t, uint64(0), res[1].Messages[0].Offset)
	})

	t.Run("Combinations", func(t *testing.T) {
		res := combineBatches([]parsers.MessageBatch{
			{Topic: "/directory/test-topic", Partition: 0, Messages: []parsers.Message{{Offset: 0}}},
			{Topic: "/directory/test-topic", Partition: 1, Messages: []parsers.Message{{Offset: 0}}},
			{Topic: "/directory/other-topic", Partition: 0, Messages: []parsers.Message{{Offset: 0}}},
			{Topic: "/directory/test-topic", Partition: 1, Messages: []parsers.Message{{Offset: 1}}},
		})

		require.Len(t, res, 3)

		require.Len(t, res[0].Messages, 1)
		require.Equal(t, uint64(0), res[0].Messages[0].Offset)

		require.Len(t, res[1].Messages, 2)
		require.Equal(t, uint64(0), res[1].Messages[0].Offset)
		require.Equal(t, uint64(1), res[1].Messages[1].Offset)

		require.Len(t, res[2].Messages, 1)
		require.Equal(t, uint64(0), res[2].Messages[0].Offset)
	})
}
