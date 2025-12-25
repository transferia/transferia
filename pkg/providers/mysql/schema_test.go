package mysql

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

func Test_keyNamesFromConstraints(t *testing.T) {
	t.Run("NoKeysAndConstraints", func(t *testing.T) {
		var testConstraintCols []constraintColumn

		res := keyNamesFromConstraints(testConstraintCols)

		require.Len(t, res, 0)
	})

	t.Run("PrimaryKeys", func(t *testing.T) {
		testConstraintCols := []constraintColumn{
			{
				Col:            abstract.ColSchema{TableSchema: "public", TableName: "test", ColumnName: "id_1"},
				ConstraintName: sql.NullString{String: "PRIMARY", Valid: true},
				Position:       0,
			},
			{
				Col:            abstract.ColSchema{TableSchema: "public", TableName: "test", ColumnName: "id_2"},
				ConstraintName: sql.NullString{String: "PRIMARY", Valid: true},
				Position:       1,
			},
			{
				Col:            abstract.ColSchema{TableSchema: "public", TableName: "test_1", ColumnName: "id"},
				ConstraintName: sql.NullString{String: "PRIMARY", Valid: true},
				Position:       0,
			},
		}

		res := keyNamesFromConstraints(testConstraintCols)

		require.Len(t, res, 2)
		keys, ok := res[abstract.TableID{Namespace: "public", Name: "test"}]
		require.True(t, ok)
		require.Equal(t, []string{"id_1", "id_2"}, keys)
		keys, ok = res[abstract.TableID{Namespace: "public", Name: "test_1"}]
		require.True(t, ok)
		require.Equal(t, []string{"id"}, keys)
	})

	t.Run("UniqueConstraints", func(t *testing.T) {
		testConstraintCols := []constraintColumn{
			{
				Col:            abstract.ColSchema{TableSchema: "public", TableName: "test", ColumnName: "id_1"},
				ConstraintName: sql.NullString{String: "a", Valid: true},
				Position:       0,
			},
			{
				Col:            abstract.ColSchema{TableSchema: "public", TableName: "test", ColumnName: "id_3"},
				ConstraintName: sql.NullString{String: "a", Valid: true},
				Position:       1,
			},
			{
				Col:            abstract.ColSchema{TableSchema: "public", TableName: "test", ColumnName: "id_2"},
				ConstraintName: sql.NullString{String: "b", Valid: true},
				Position:       0,
			},
			{
				Col:            abstract.ColSchema{TableSchema: "public", TableName: "test", ColumnName: "id_4"},
				ConstraintName: sql.NullString{String: "b", Valid: true},
				Position:       1,
			},
			{
				Col:            abstract.ColSchema{TableSchema: "public", TableName: "test", ColumnName: "id_1"},
				ConstraintName: sql.NullString{String: "c", Valid: true},
				Position:       0,
			},
			{
				Col:            abstract.ColSchema{TableSchema: "public", TableName: "test_1", ColumnName: "id"},
				ConstraintName: sql.NullString{String: "a", Valid: true},
				Position:       0,
			},
		}

		res := keyNamesFromConstraints(testConstraintCols)

		require.Len(t, res, 2)
		keys, ok := res[abstract.TableID{Namespace: "public", Name: "test"}]
		require.True(t, ok)
		require.Equal(t, []string{"id_1", "id_2", "id_3", "id_4"}, keys)

		keys, ok = res[abstract.TableID{Namespace: "public", Name: "test_1"}]
		require.True(t, ok)
		require.Equal(t, []string{"id"}, keys)
	})

	t.Run("PrimaryKeysAndUniqueConstraints", func(t *testing.T) {
		testConstraintCols := []constraintColumn{
			{
				Col:            abstract.ColSchema{TableSchema: "public", TableName: "test", ColumnName: "id_1"},
				ConstraintName: sql.NullString{String: "a", Valid: true},
				Position:       0,
			},
			{
				Col:            abstract.ColSchema{TableSchema: "public", TableName: "test", ColumnName: "id_3"},
				ConstraintName: sql.NullString{String: "a", Valid: true},
				Position:       1,
			},
			{
				Col:            abstract.ColSchema{TableSchema: "public", TableName: "test", ColumnName: "id"},
				ConstraintName: sql.NullString{String: "PRIMARY", Valid: true},
				Position:       0,
			},
		}

		res := keyNamesFromConstraints(testConstraintCols)

		require.Len(t, res, 1)
		keys, ok := res[abstract.TableID{Namespace: "public", Name: "test"}]
		require.True(t, ok)
		require.Equal(t, []string{"id"}, keys)
	})
}
