package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/internal/metrics"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/generic"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestFallbackConsistentWithGenericParser(t *testing.T) {
	fields := []abstract.ColSchema{{
		ColumnName: "id",
		DataType:   schema.TypeInt8.String(),
	}}

	parserConfig := &generic.GenericParserConfig{
		Format:             "json",
		SchemaResourceName: "",
		Fields:             fields,
		AuxOpts: generic.AuxParserOpts{
			Topic:                  "my_topic_name",
			AddDedupeKeys:          true,
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
			UnescapeStringValues:   false,
			UnpackBytesBase64:      false,
		},
	}

	parser := generic.NewGenericParser(
		parserConfig,
		fields,
		logger.Log,
		stats.NewSourceStats(metrics.NewRegistry().WithTags(map[string]string{"id": "TestParser_Do"})),
	)

	t.Run("check parsed", func(t *testing.T) {
		checkParsedChangeItem := func(t *testing.T, changeItem abstract.ChangeItem) {
			require.Equal(t, "my_topic_name", changeItem.Table)
			require.Len(t, changeItem.TableSchema.Columns(), 5)
			require.Len(t, changeItem.ColumnNames, 5)
			require.Equal(t, changeItem.TableSchema.Columns()[1].ColumnName, generic.ColNameTimestamp)
			require.Equal(t, changeItem.TableSchema.Columns()[2].ColumnName, generic.ColNamePartition)
			require.Equal(t, changeItem.TableSchema.Columns()[3].ColumnName, generic.ColNameOffset)
			require.Equal(t, changeItem.TableSchema.Columns()[4].ColumnName, generic.ColNameIdx)
		}

		changeItems := parser.Do(parsers.Message{Value: []byte(`{"id":1}`)}, abstract.Partition{})
		require.Len(t, changeItems, 1)
		require.True(t, isParsedItem(&changeItems[0]))
		require.False(t, isUnparsedItem(&changeItems[0]))
		checkParsedChangeItem(t, changeItems[0])

		require.Equal(t, schema.TypeTimestamp.String(), changeItems[0].TableSchema.Columns()[1].DataType)
		changeItem, err := GenericParserTimestampFallback(&changeItems[0])
		require.NoError(t, err)

		checkParsedChangeItem(t, *changeItem)
		require.Equal(t, changeItem.TableSchema.Columns()[1].DataType, schema.TypeDatetime.String())
	})

	t.Run("check unparsed", func(t *testing.T) {
		checkUnparsedChangeItem := func(t *testing.T, changeItem abstract.ChangeItem) {
			require.Equal(t, "my_topic_name_unparsed", changeItem.Table)
			require.Len(t, changeItem.TableSchema.Columns(), 6)
			require.Len(t, changeItem.ColumnNames, 6)
			require.Equal(t, changeItem.TableSchema.Columns()[0].ColumnName, generic.ColNameTimestamp)
			require.Equal(t, changeItem.TableSchema.Columns()[1].ColumnName, generic.ColNamePartition)
			require.Equal(t, changeItem.TableSchema.Columns()[2].ColumnName, generic.ColNameOffset)
			require.Equal(t, changeItem.TableSchema.Columns()[3].ColumnName, generic.ColNameIdx)
		}

		changeItems := parser.Do(parsers.Message{Value: []byte(`{"id":1`)}, abstract.Partition{})
		require.Len(t, changeItems, 1)
		require.False(t, isParsedItem(&changeItems[0]))
		require.True(t, isUnparsedItem(&changeItems[0]))
		checkUnparsedChangeItem(t, changeItems[0])

		require.Equal(t, schema.TypeTimestamp.String(), changeItems[0].TableSchema.Columns()[0].DataType)
		changeItem, err := GenericParserTimestampFallback(&changeItems[0])
		require.NoError(t, err)

		checkUnparsedChangeItem(t, *changeItem)
		require.Equal(t, changeItem.TableSchema.Columns()[0].DataType, schema.TypeDatetime.String())
	})
}
