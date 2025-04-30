package mongo_pk_extender

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"go.mongodb.org/mongo-driver/bson"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestMongoPKExtenderTransformer(t *testing.T) {
	tableSchema := abstract.NewTableSchema(abstract.TableColumns{
		abstract.MakeTypedColSchema("_id", string(schema.TypeString), true),
		abstract.MakeTypedColSchema("document", string(schema.TypeString), false),
	})

	t.Run("Expand PK", func(t *testing.T) {
		config := Config{
			Expand:          true,
			ExtraFieldName:  "orgId",
			ExtraFieldValue: "42",
		}
		transformer, err := NewMongoPKExtenderTransformer(config, logger.Log)
		require.NoError(t, err)

		insertSimple := abstract.ChangeItem{
			Kind:         abstract.InsertKind,
			Schema:       "db",
			Table:        "table",
			TableSchema:  tableSchema,
			ColumnNames:  []string{"_id", "document"},
			ColumnValues: []interface{}{15, bson.D{{Key: "name", Value: "John"}, {Key: "age", Value: 30}}},
		}

		insertComposite := abstract.ChangeItem{
			Kind:         abstract.InsertKind,
			Schema:       "db",
			Table:        "table",
			TableSchema:  tableSchema,
			ColumnNames:  []string{"_id", "document"},
			ColumnValues: []interface{}{bson.D{{Key: "id", Value: 1}, {Key: "category", Value: "test"}}, bson.D{{Key: "name", Value: "John"}, {Key: "age", Value: 30}}},
		}

		changeItems := []abstract.ChangeItem{
			insertSimple, insertComposite,
		}
		result := transformer.Apply(changeItems)
		require.Len(t, result.Transformed, 2)
		require.Equal(t, bson.D{{Key: "orgId", Value: int64(42)}, {Key: "id", Value: 15}}, result.Transformed[0].ColumnValues[0])
		require.Equal(t, bson.D{{Key: "name", Value: "John"}, {Key: "age", Value: 30}}, result.Transformed[0].ColumnValues[1])
		require.Len(t, result.Transformed[0].OldKeys.KeyValues, 1)
		require.Equal(t, bson.D{{Key: "orgId", Value: int64(42)}, {Key: "id", Value: 15}}, result.Transformed[0].OldKeys.KeyValues[0])

		require.Equal(t, bson.D{{Key: "orgId", Value: int64(42)}, {Key: "id", Value: bson.D{{Key: "id", Value: 1}, {Key: "category", Value: "test"}}}}, result.Transformed[1].ColumnValues[0])
		require.Equal(t, bson.D{{Key: "name", Value: "John"}, {Key: "age", Value: 30}}, result.Transformed[1].ColumnValues[1])
		require.Len(t, result.Transformed[1].OldKeys.KeyValues, 1)
		require.Equal(t, bson.D{{Key: "orgId", Value: int64(42)}, {Key: "id", Value: bson.D{{Key: "id", Value: 1}, {Key: "category", Value: "test"}}}}, result.Transformed[1].OldKeys.KeyValues[0])
	})

	t.Run("Collapse PK", func(t *testing.T) {
		config := Config{
			Expand:          false,
			ExtraFieldName:  "orgId",
			ExtraFieldValue: "42",
		}
		transformer, err := NewMongoPKExtenderTransformer(config, logger.Log)
		require.NoError(t, err)

		insertSimple := abstract.ChangeItem{
			Kind:         abstract.InsertKind,
			Schema:       "db",
			Table:        "table",
			TableSchema:  tableSchema,
			ColumnNames:  []string{"_id", "document"},
			ColumnValues: []interface{}{bson.D{{Key: "orgId", Value: 42}, {Key: "id", Value: 15}}, bson.D{{Key: "name", Value: "John"}, {Key: "age", Value: 30}}},
		}

		insertComposite := abstract.ChangeItem{
			Kind:         abstract.InsertKind,
			Schema:       "db",
			Table:        "table",
			TableSchema:  tableSchema,
			ColumnNames:  []string{"_id", "document"},
			ColumnValues: []interface{}{bson.D{{Key: "orgId", Value: int64(42)}, {Key: "id", Value: bson.D{{Key: "id", Value: 1}, {Key: "category", Value: "test"}}}}, bson.D{{Key: "name", Value: "John"}, {Key: "age", Value: 30}}},
		}

		insertOtherField := abstract.ChangeItem{
			Kind:         abstract.InsertKind,
			Schema:       "db",
			Table:        "table",
			TableSchema:  tableSchema,
			ColumnNames:  []string{"_id", "document"},
			ColumnValues: []interface{}{bson.D{{Key: "departmentId", Value: int64(42)}, {Key: "id", Value: bson.D{{Key: "id", Value: 1}, {Key: "category", Value: "test"}}}}, bson.D{{Key: "name", Value: "John"}, {Key: "age", Value: 30}}},
		}

		insertOtherValue := abstract.ChangeItem{
			Kind:         abstract.InsertKind,
			Schema:       "db",
			Table:        "table",
			TableSchema:  tableSchema,
			ColumnNames:  []string{"_id", "document"},
			ColumnValues: []interface{}{bson.D{{Key: "orgId", Value: int64(125)}, {Key: "id", Value: bson.D{{Key: "id", Value: 1}, {Key: "category", Value: "test"}}}}, bson.D{{Key: "name", Value: "John"}, {Key: "age", Value: 30}}},
		}

		changeItems := []abstract.ChangeItem{
			insertSimple, insertComposite, insertOtherField, insertOtherValue,
		}
		result := transformer.Apply(changeItems)
		require.Len(t, result.Transformed, 2)
		require.Equal(t, 15, result.Transformed[0].ColumnValues[0])
		require.Equal(t, bson.D{{Key: "name", Value: "John"}, {Key: "age", Value: 30}}, result.Transformed[0].ColumnValues[1])
		require.Len(t, result.Transformed[0].OldKeys.KeyValues, 1)
		require.Equal(t, 15, result.Transformed[0].OldKeys.KeyValues[0])

		require.Equal(t, bson.D{{Key: "id", Value: 1}, {Key: "category", Value: "test"}}, result.Transformed[1].ColumnValues[0])
		require.Equal(t, bson.D{{Key: "name", Value: "John"}, {Key: "age", Value: 30}}, result.Transformed[1].ColumnValues[1])
		require.Len(t, result.Transformed[1].OldKeys.KeyValues, 1)
		require.Equal(t, bson.D{{Key: "id", Value: 1}, {Key: "category", Value: "test"}}, result.Transformed[1].OldKeys.KeyValues[0])
	})
}
