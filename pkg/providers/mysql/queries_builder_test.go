//go:build !disable_mysql_provider

package mysql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/stats"
)

func makeStubSinker() sinker {
	return sinker{
		db:      nil,
		metrics: stats.NewSinkerStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
		config: &MysqlDestination{
			Password:  "",
			Host:      "",
			User:      "",
			ClusterID: "",
		},
		logger: logger.Log,
	}
}

var tableSchema = abstract.NewTableSchema([]abstract.ColSchema{
	{ColumnName: "id", PrimaryKey: true},
	{ColumnName: "str", PrimaryKey: false},
	{ColumnName: "str2", PrimaryKey: false},
})

func Test_buildQueries00(t *testing.T) {
	t.Run("delete", func(t *testing.T) {
		changeItems := []abstract.ChangeItem{
			{
				Kind:         "delete",
				Schema:       "db",
				Table:        "myTableName",
				ColumnNames:  []string{"id"},
				ColumnValues: []interface{}{1},
				OldKeys: abstract.OldKeysType{
					KeyNames:  []string{"id"},
					KeyValues: []interface{}{1},
				},
			},
		}

		tableID := abstract.TableID{
			Namespace: "db",
			Name:      "myTableName",
		}

		s := makeStubSinker()
		expected := []sinkQuery{
			*newSinkQuery("SET FOREIGN_KEY_CHECKS=1;\n", false),
			*newSinkQuery("DELETE FROM `db`.`myTableName` WHERE (`id`=1);", false),
		}
		queries, err := s.buildQueries(tableID, tableSchema.Columns(), changeItems)
		require.NoError(t, err)
		require.Equal(t, expected, queries)
	})
}

func Test_buildQueries01(t *testing.T) {
	t.Run("update primary key", func(t *testing.T) {
		changeItems := []abstract.ChangeItem{
			{
				Kind:         "update",
				Schema:       "db",
				Table:        "myTableName",
				ColumnNames:  []string{"id"},
				ColumnValues: []interface{}{2},
				OldKeys: abstract.OldKeysType{
					KeyNames:  []string{"id"},
					KeyValues: []interface{}{1},
				},
			},
		}

		tableID := abstract.TableID{
			Namespace: "db",
			Name:      "myTableName",
		}

		s := makeStubSinker()
		queries, err := s.buildQueries(tableID, tableSchema.Columns(), changeItems)
		require.NoError(t, err)
		require.Equal(t, []sinkQuery{*newSinkQuery("UPDATE IGNORE `db`.`myTableName` SET `id` = 2 WHERE `id` = 1;", false)}, queries)
	})
}

func Test_buildQueries02(t *testing.T) {
	t.Run("update full line", func(t *testing.T) {
		changeItems := []abstract.ChangeItem{
			{
				Kind:         "update",
				Schema:       "db",
				Table:        "myTableName",
				ColumnNames:  []string{"id", "str", "str2"},
				ColumnValues: []interface{}{1, "v", "v"},
				OldKeys: abstract.OldKeysType{
					KeyNames:  []string{"id", "str", "str2"},
					KeyValues: []interface{}{1, "z", "v"},
				},
			},
		}

		tableID := abstract.TableID{
			Namespace: "db",
			Name:      "myTableName",
		}

		s := makeStubSinker()
		queries, err := s.buildQueries(tableID, tableSchema.Columns(), changeItems)
		require.NoError(t, err)
		require.Equal(t, []sinkQuery{
			*newSinkQuery("SET FOREIGN_KEY_CHECKS=0;\n", false),
			*newSinkQuery("INSERT INTO `db`.`myTableName` (`id`,`str`,`str2`) VALUES\n(1,'v','v')\nON DUPLICATE KEY UPDATE \n `str` = VALUES(`str`),\n`str2` = VALUES(`str2`)\n;", true),
		}, queries)
	})
}

func Test_buildQueries03(t *testing.T) {
	t.Run("update - when str2 toasted", func(t *testing.T) {
		changeItems := []abstract.ChangeItem{
			{
				Kind:         "update",
				Schema:       "db",
				Table:        "myTableName",
				ColumnNames:  []string{"id", "str"},
				ColumnValues: []interface{}{1, "v"},
				OldKeys: abstract.OldKeysType{
					KeyNames:  []string{"id", "str"},
					KeyValues: []interface{}{1, "z"},
				},
			},
		}

		tableID := abstract.TableID{
			Namespace: "db",
			Name:      "myTableName",
		}

		s := makeStubSinker()
		queries, err := s.buildQueries(tableID, tableSchema.Columns(), changeItems)
		require.NoError(t, err)
		expected := []sinkQuery{*newSinkQuery("UPDATE IGNORE `db`.`myTableName` SET `str` = 'v' WHERE `id` = 1;", false)}
		require.Equal(t, expected, queries)
	})
}

func Test_buildQueries04(t *testing.T) {
	t.Run("insert full line", func(t *testing.T) {
		changeItems := []abstract.ChangeItem{
			{
				Kind:         "insert",
				Schema:       "db",
				Table:        "myTableName",
				ColumnNames:  []string{"id", "str", "str2"},
				ColumnValues: []interface{}{1, "v", "v"},
			},
		}

		tableID := abstract.TableID{
			Namespace: "db",
			Name:      "myTableName",
		}

		s := makeStubSinker()
		queries, err := s.buildQueries(tableID, tableSchema.Columns(), changeItems)
		require.NoError(t, err)
		expected := []sinkQuery{
			*newSinkQuery("SET FOREIGN_KEY_CHECKS=0;\n", false),
			*newSinkQuery("INSERT INTO `db`.`myTableName` (`id`,`str`,`str2`) VALUES\n(1,'v','v')\nON DUPLICATE KEY UPDATE \n `str` = VALUES(`str`),\n`str2` = VALUES(`str2`)\n;", true),
		}
		require.Equal(t, expected, queries)
	})
}

func Test_buildQueries05(t *testing.T) {
	t.Run("insert - case when len(conflictUpd)==0. It means we don't have usual fields (not a pkey & not a generated column)", func(t *testing.T) {
		currTableSchema := []abstract.ColSchema{
			{ColumnName: "id", PrimaryKey: true},
			{ColumnName: "str", PrimaryKey: true},
		}
		changeItems := []abstract.ChangeItem{
			{
				Kind:         "insert",
				Schema:       "db",
				Table:        "myTableName",
				ColumnNames:  []string{"id", "str"},
				ColumnValues: []interface{}{1, "v"},
			},
		}

		tableID := abstract.TableID{
			Namespace: "db",
			Name:      "myTableName",
		}

		s := makeStubSinker()
		queries, err := s.buildQueries(tableID, currTableSchema, changeItems)
		require.NoError(t, err)
		require.Equal(t, []sinkQuery{
			*newSinkQuery("SET FOREIGN_KEY_CHECKS=0;\n", false),
			*newSinkQuery("INSERT IGNORE INTO `db`.`myTableName` (`id`,`str`) VALUES\n(1,'v');", true),
		}, queries)
	})
}

func Test_buildQueries06(t *testing.T) {
	t.Run("insert - when table have uniqConstraints", func(t *testing.T) {
		changeItems := []abstract.ChangeItem{
			{
				Kind:         "insert",
				Schema:       "db",
				Table:        "myTableName",
				ColumnNames:  []string{"id", "str", "str2"},
				ColumnValues: []interface{}{1, "v", "v"},
			},
		}

		tableID := abstract.TableID{
			Namespace: "db",
			Name:      "myTableName",
		}

		s := makeStubSinker()
		s.uniqConstraints = make(map[string]bool)
		s.uniqConstraints[tableID.Fqtn()] = true
		queries, err := s.buildQueries(tableID, tableSchema.Columns(), changeItems)
		require.NoError(t, err)
		require.Equal(t, []sinkQuery{
			*newSinkQuery("SET FOREIGN_KEY_CHECKS=0;\n", false),
			*newSinkQuery("REPLACE `db`.`myTableName` (`id`,`str`,`str2`) VALUES\n(1,'v','v');", false),
		}, queries)
	})
}

func Test_buildQueries_big(t *testing.T) {
	changeItems := []abstract.ChangeItem{
		// insert full line
		{
			ID:           1,
			Kind:         "insert",
			Schema:       "db",
			Table:        "myTableName",
			TableSchema:  tableSchema,
			ColumnNames:  []string{"id", "str", "str2"},
			ColumnValues: []interface{}{2, "v2", "v2"},
		},
		// delete
		{
			ID:           2,
			Kind:         "delete",
			Schema:       "db",
			Table:        "myTableName",
			TableSchema:  tableSchema,
			ColumnNames:  []string{"id"},
			ColumnValues: []interface{}{2},
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"id"},
				KeyValues: []interface{}{2},
			},
		},
		// update primary key
		{
			ID:           3,
			Kind:         "update",
			Schema:       "db",
			Table:        "myTableName",
			TableSchema:  tableSchema,
			ColumnNames:  []string{"id"},
			ColumnValues: []interface{}{4},
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"id"},
				KeyValues: []interface{}{3},
			},
		},
		// update full line
		{
			ID:           4,
			Kind:         "update",
			Schema:       "db",
			Table:        "myTableName",
			TableSchema:  tableSchema,
			ColumnNames:  []string{"id", "str", "str2"},
			ColumnValues: []interface{}{1, "v1", "v1"},
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"id"},
				KeyValues: []interface{}{1},
			},
		},
		// it's like toasted str2
		{
			ID:           5,
			Kind:         "update",
			Schema:       "db",
			Table:        "myTableName",
			TableSchema:  tableSchema,
			ColumnNames:  []string{"id", "str"},
			ColumnValues: []interface{}{1, "v11"},
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"id"},
				KeyValues: []interface{}{1},
			},
		},
	}

	//---

	tableID := abstract.TableID{
		Namespace: "db",
		Name:      "myTableName",
	}

	s := makeStubSinker()
	queries, err := s.buildQueries(tableID, tableSchema.Columns(), changeItems)
	require.NoError(t, err)
	expected := []sinkQuery{
		*newSinkQuery("SET FOREIGN_KEY_CHECKS=0;\n", false),
		*newSinkQuery("INSERT INTO `db`.`myTableName` (`id`,`str`,`str2`) VALUES\n(2,'v2','v2')\nON DUPLICATE KEY UPDATE \n `str` = VALUES(`str`),\n`str2` = VALUES(`str2`)\n;", true),
		*newSinkQuery("SET FOREIGN_KEY_CHECKS=1;\n", false),
		*newSinkQuery("DELETE FROM `db`.`myTableName` WHERE (`id`=2);", false),
		*newSinkQuery("UPDATE IGNORE `db`.`myTableName` SET `id` = 4 WHERE `id` = 3;", false),
		*newSinkQuery("SET FOREIGN_KEY_CHECKS=0;\n", false),
		*newSinkQuery("INSERT INTO `db`.`myTableName` (`id`,`str`,`str2`) VALUES\n(1,'v1','v1')\nON DUPLICATE KEY UPDATE \n `str` = VALUES(`str`),\n`str2` = VALUES(`str2`)\n;", true),
		*newSinkQuery("UPDATE IGNORE `db`.`myTableName` SET `str` = 'v11' WHERE `id` = 1;", false),
	}
	require.Equal(t, expected, queries)
}

//---------------------------------------------------------------------------------------------------------------------

func Test_makeMapColnameToIndex(t *testing.T) {
	tableSchema00 := []abstract.ColSchema{{ColumnName: "a"}}
	require.Equal(t, map[string]int{"a": 0}, columnNameToIndex(&tableSchema00))

	tableSchema01 := []abstract.ColSchema{{ColumnName: "a"}, {ColumnName: "b"}}
	require.Equal(t, map[string]int{"a": 0, "b": 1}, columnNameToIndex(&tableSchema01))
}

func reverseArr(a []abstract.ColSchema) []abstract.ColSchema {
	for i, j := 0, len(a)-1; i < j; i, j = i+1, j-1 {
		a[i], a[j] = a[j], a[i]
	}
	return a
}

func Test_buildPartOfQueryDeleteCondition(t *testing.T) {
	t.Run("one column - PrimaryKey", func(t *testing.T) {
		tableSchema00 := []abstract.ColSchema{{ColumnName: "a", PrimaryKey: true}}
		changeItem00 := abstract.ChangeItem{
			Kind:         "delete",
			ColumnNames:  []string{"a"},
			ColumnValues: []interface{}{1},
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"a"},
				KeyValues: []interface{}{123},
			},
		}

		mapColnameToIndex00 := columnNameToIndex(&tableSchema00)
		require.Equal(
			t,
			"(`a`=123)",
			buildPartOfQueryDeleteCondition(&tableSchema00, &changeItem00, &mapColnameToIndex00),
		)

		// reverse order of tableSchema - to ensure that tested func works correctly with another order between changeItem & tableSchema
		tableSchema00 = reverseArr(tableSchema00)
		mapColnameToIndex00r := columnNameToIndex(&tableSchema00)
		require.Equal(
			t,
			"(`a`=123)",
			buildPartOfQueryDeleteCondition(&tableSchema00, &changeItem00, &mapColnameToIndex00r),
		)

		// check it will work fine - if #cols in TableSchema more than in changeItem
		tableSchema00 = append(tableSchema00, abstract.ColSchema{ColumnName: "X", PrimaryKey: false})
		mapColnameToIndex00x := columnNameToIndex(&tableSchema00)
		require.Equal(
			t,
			"(`a`=123)",
			buildPartOfQueryDeleteCondition(&tableSchema00, &changeItem00, &mapColnameToIndex00x),
		)
	})

	t.Run(" one column - not a PrimaryKey", func(t *testing.T) {
		tableSchema01 := []abstract.ColSchema{{ColumnName: "a", PrimaryKey: false}}
		changeItem01 := abstract.ChangeItem{
			Kind:         "delete",
			ColumnNames:  []string{"a"},
			ColumnValues: []interface{}{1},
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"a"},
				KeyValues: []interface{}{2},
			},
		}

		mapColnameToIndex01 := columnNameToIndex(&tableSchema01)
		require.Equal(
			t,
			"(`a`=2)",
			buildPartOfQueryDeleteCondition(&tableSchema01, &changeItem01, &mapColnameToIndex01),
		)

		// reverse order of tableSchema - to ensure that tested func works correctly with another order between changeItem & tableSchema
		tableSchema01 = reverseArr(tableSchema01)
		mapColnameToIndex01r := columnNameToIndex(&tableSchema01)
		require.Equal(
			t,
			"(`a`=2)",
			buildPartOfQueryDeleteCondition(&tableSchema01, &changeItem01, &mapColnameToIndex01r),
		)

		// check it will work fine - if #cols in TableSchema more than in changeItem
		tableSchema01 = append(tableSchema01, abstract.ColSchema{ColumnName: "X", PrimaryKey: false})
		mapColnameToIndex01x := columnNameToIndex(&tableSchema01)
		require.Equal(
			t,
			"(`a`=2)",
			buildPartOfQueryDeleteCondition(&tableSchema01, &changeItem01, &mapColnameToIndex01x),
		)
	})

	t.Run("two columns: both PrimaryKey", func(t *testing.T) {
		tableSchema02 := []abstract.ColSchema{
			{ColumnName: "a", PrimaryKey: true},
			{ColumnName: "b", PrimaryKey: true},
		}
		changeItem02 := abstract.ChangeItem{
			Kind:         "delete",
			ColumnNames:  []string{"a", "b"},
			ColumnValues: []interface{}{1, 2},
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"a", "b"},
				KeyValues: []interface{}{123, 234},
			},
		}

		mapColnameToIndex02 := columnNameToIndex(&tableSchema02)
		require.Equal(
			t,
			"(`a`=123 AND `b`=234)",
			buildPartOfQueryDeleteCondition(&tableSchema02, &changeItem02, &mapColnameToIndex02),
		)

		// reverse order of tableSchema - to ensure that tested func works correctly with another order between changeItem & tableSchema
		tableSchema02 = reverseArr(tableSchema02)
		mapColnameToIndex02r := columnNameToIndex(&tableSchema02)
		require.Equal(
			t,
			"(`a`=123 AND `b`=234)",
			buildPartOfQueryDeleteCondition(&tableSchema02, &changeItem02, &mapColnameToIndex02r),
		)

		// check it will work fine - if #cols in TableSchema more than in changeItem
		tableSchema02 = append(tableSchema02, abstract.ColSchema{ColumnName: "X", PrimaryKey: false})
		mapColnameToIndex02x := columnNameToIndex(&tableSchema02)
		require.Equal(
			t,
			"(`a`=123 AND `b`=234)",
			buildPartOfQueryDeleteCondition(&tableSchema02, &changeItem02, &mapColnameToIndex02x),
		)
	})

	t.Run("two columns: only one PrimaryKey (1st col)", func(t *testing.T) {
		tableSchema03 := []abstract.ColSchema{
			{ColumnName: "a", PrimaryKey: true},
			{ColumnName: "b", PrimaryKey: false},
		}
		changeItem03 := abstract.ChangeItem{
			Kind:         "delete",
			ColumnNames:  []string{"a", "b"},
			ColumnValues: []interface{}{1, 2},
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"a", "b"},
				KeyValues: []interface{}{123, 234},
			},
		}

		mapColnameToIndex03 := columnNameToIndex(&tableSchema03)
		require.Equal(
			t,
			"(`a`=123 AND `b`=234)",
			buildPartOfQueryDeleteCondition(&tableSchema03, &changeItem03, &mapColnameToIndex03),
		)

		// reverse order of tableSchema - to ensure that tested func works correctly with another order between changeItem & tableSchema
		tableSchema03 = reverseArr(tableSchema03)
		mapColnameToIndex03r := columnNameToIndex(&tableSchema03)
		require.Equal(
			t,
			"(`a`=123 AND `b`=234)",
			buildPartOfQueryDeleteCondition(&tableSchema03, &changeItem03, &mapColnameToIndex03r),
		)

		// check it will work fine - if #cols in TableSchema more than in changeItem
		tableSchema03 = append(tableSchema03, abstract.ColSchema{ColumnName: "X", PrimaryKey: false})
		mapColnameToIndex03x := columnNameToIndex(&tableSchema03)
		require.Equal(
			t,
			"(`a`=123 AND `b`=234)",
			buildPartOfQueryDeleteCondition(&tableSchema03, &changeItem03, &mapColnameToIndex03x),
		)
	})

	t.Run("two columns: only one PrimaryKey (2nd col)", func(t *testing.T) {
		tableSchema04 := []abstract.ColSchema{
			{ColumnName: "a", PrimaryKey: false},
			{ColumnName: "b", PrimaryKey: true},
		}
		changeItem04 := abstract.ChangeItem{
			Kind:         "delete",
			ColumnNames:  []string{"a", "b"},
			ColumnValues: []interface{}{1, 2},
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"a", "b"},
				KeyValues: []interface{}{123, 234},
			},
		}

		mapColnameToIndex04 := columnNameToIndex(&tableSchema04)
		require.Equal(
			t,
			"(`a`=123 AND `b`=234)",
			buildPartOfQueryDeleteCondition(&tableSchema04, &changeItem04, &mapColnameToIndex04),
		)

		// reverse order of tableSchema - to ensure that tested func works correctly with another order between changeItem & tableSchema
		tableSchema04 = reverseArr(tableSchema04)
		mapColnameToIndex04r := columnNameToIndex(&tableSchema04)
		require.Equal(
			t,
			"(`a`=123 AND `b`=234)",
			buildPartOfQueryDeleteCondition(&tableSchema04, &changeItem04, &mapColnameToIndex04r),
		)

		// check it will work fine - if #cols in TableSchema more than in changeItem
		tableSchema04 = append(tableSchema04, abstract.ColSchema{ColumnName: "X", PrimaryKey: false})
		mapColnameToIndex04x := columnNameToIndex(&tableSchema04)
		require.Equal(
			t,
			"(`a`=123 AND `b`=234)",
			buildPartOfQueryDeleteCondition(&tableSchema04, &changeItem04, &mapColnameToIndex04x),
		)
	})

	t.Run("two columns: no one PrimaryKey", func(t *testing.T) {
		tableSchema05 := []abstract.ColSchema{
			{ColumnName: "a", PrimaryKey: false},
			{ColumnName: "b", PrimaryKey: false},
		}
		changeItem05 := abstract.ChangeItem{
			Kind:         "delete",
			ColumnNames:  []string{"a", "b"},
			ColumnValues: []interface{}{1, 2},
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"a", "b"},
				KeyValues: []interface{}{2, 3},
			},
		}

		mapColnameToIndex05 := columnNameToIndex(&tableSchema05)
		require.Equal(
			t,
			"(`a`=2 AND `b`=3)",
			buildPartOfQueryDeleteCondition(&tableSchema05, &changeItem05, &mapColnameToIndex05),
		)

		// reverse order of tableSchema - to ensure that tested func works correctly with another order between changeItem & tableSchema
		tableSchema05 = reverseArr(tableSchema05)
		mapColnameToIndex05r := columnNameToIndex(&tableSchema05)
		require.Equal(
			t,
			"(`a`=2 AND `b`=3)",
			buildPartOfQueryDeleteCondition(&tableSchema05, &changeItem05, &mapColnameToIndex05r),
		)

		// check it will work fine - if #cols in TableSchema more than in changeItem
		tableSchema05 = append(tableSchema05, abstract.ColSchema{ColumnName: "X", PrimaryKey: false})
		mapColnameToIndex05x := columnNameToIndex(&tableSchema05)
		require.Equal(
			t,
			"(`a`=2 AND `b`=3)",
			buildPartOfQueryDeleteCondition(&tableSchema05, &changeItem05, &mapColnameToIndex05x),
		)
	})
}

func Test_buildQueryUpdate(t *testing.T) {
	t.Run("one column - PrimaryKey, changed", func(t *testing.T) {
		tableSchema00 := []abstract.ColSchema{{ColumnName: "a", PrimaryKey: true}}
		changeItem00 := abstract.ChangeItem{
			Kind:         "update",
			Schema:       "db",
			Table:        "my_table_name_00",
			ColumnNames:  []string{"a"},
			ColumnValues: []interface{}{1},
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"a"},
				KeyValues: []interface{}{123},
			},
		}

		tableID := abstract.TableID{
			Namespace: "db",
			Name:      "my_table_name_00",
		}

		mapColnameToIndex00 := columnNameToIndex(&tableSchema00)
		str00, isAdded00 := buildQueryUpdate(tableID, &tableSchema00, &changeItem00, &mapColnameToIndex00)
		require.Equal(t, true, isAdded00)
		require.Equal(t, "UPDATE IGNORE `db`.`my_table_name_00` SET `a` = 1 WHERE `a` = 123;", str00)

		// reverse order of tableSchema - to ensure that tested func works correctly with another order between changeItem & tableSchema
		tableSchema00 = reverseArr(tableSchema00)
		mapColnameToIndex00r := columnNameToIndex(&tableSchema00)
		str00r, isAdded00r := buildQueryUpdate(tableID, &tableSchema00, &changeItem00, &mapColnameToIndex00r)
		require.Equal(t, true, isAdded00r)
		require.Equal(t, "UPDATE IGNORE `db`.`my_table_name_00` SET `a` = 1 WHERE `a` = 123;", str00r)
	})

	t.Run("one column - not a PrimaryKey, changed", func(t *testing.T) {
		tableSchema01 := []abstract.ColSchema{{ColumnName: "a", PrimaryKey: false}}
		changeItem01 := abstract.ChangeItem{
			Kind:         "update",
			Schema:       "db",
			Table:        "my_table_name_00",
			ColumnNames:  []string{"a"},
			ColumnValues: []interface{}{1},
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"a"},
				KeyValues: []interface{}{123},
			},
		}

		tableID := abstract.TableID{
			Namespace: "db",
			Name:      "my_table_name_00",
		}

		mapColnameToIndex01 := columnNameToIndex(&tableSchema01)
		str01, isAdded01 := buildQueryUpdate(tableID, &tableSchema01, &changeItem01, &mapColnameToIndex01)
		require.Equal(t, true, isAdded01)
		require.Equal(t, "UPDATE IGNORE `db`.`my_table_name_00` SET `a` = 1 WHERE `a` = 123;", str01)

		// reverse order of tableSchema - to ensure that tested func works correctly with another order between changeItem & tableSchema
		tableSchema01 = reverseArr(tableSchema01)
		mapColnameToIndex01r := columnNameToIndex(&tableSchema01)
		str01r, isAdded01r := buildQueryUpdate(tableID, &tableSchema01, &changeItem01, &mapColnameToIndex01r)
		require.Equal(t, true, isAdded01r)
		require.Equal(t, "UPDATE IGNORE `db`.`my_table_name_00` SET `a` = 1 WHERE `a` = 123;", str01r)
	})

	t.Run("two columns: both PrimaryKey, one 1st changed", func(t *testing.T) {
		tableSchema02 := []abstract.ColSchema{
			{ColumnName: "a", PrimaryKey: true},
			{ColumnName: "b", PrimaryKey: true},
		}
		changeItem02 := abstract.ChangeItem{
			Kind:         "update",
			Schema:       "db",
			Table:        "my_table_name_00",
			ColumnNames:  []string{"a", "b"},
			ColumnValues: []interface{}{1, 2},
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"a", "b"},
				KeyValues: []interface{}{1, 3},
			},
		}

		tableID := abstract.TableID{
			Namespace: "db",
			Name:      "my_table_name_00",
		}

		mapColnameToIndex02 := columnNameToIndex(&tableSchema02)
		str02, isAdded02 := buildQueryUpdate(tableID, &tableSchema02, &changeItem02, &mapColnameToIndex02)
		require.Equal(t, true, isAdded02)
		require.Equal(t, "UPDATE IGNORE `db`.`my_table_name_00` SET `b` = 2 WHERE `a` = 1 AND `b` = 3;", str02)

		// reverse order of tableSchema - to ensure that tested func works correctly with another order between changeItem & tableSchema
		tableSchema02 = reverseArr(tableSchema02)
		mapColnameToIndex02r := columnNameToIndex(&tableSchema02)
		str02r, isAdded02r := buildQueryUpdate(tableID, &tableSchema02, &changeItem02, &mapColnameToIndex02r)
		require.Equal(t, true, isAdded02r)
		require.Equal(t, "UPDATE IGNORE `db`.`my_table_name_00` SET `b` = 2 WHERE `a` = 1 AND `b` = 3;", str02r)
	})

	t.Run("two columns: both PrimaryKey, one 2nd changed", func(t *testing.T) {
		tableSchema04 := []abstract.ColSchema{
			{ColumnName: "a", PrimaryKey: true},
			{ColumnName: "b", PrimaryKey: true},
		}
		changeItem04 := abstract.ChangeItem{
			Kind:         "update",
			Schema:       "db",
			Table:        "my_table_name_00",
			ColumnNames:  []string{"a", "b"},
			ColumnValues: []interface{}{1, 2},
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"a", "b"},
				KeyValues: []interface{}{2, 2},
			},
		}

		tableID := abstract.TableID{
			Namespace: "db",
			Name:      "my_table_name_00",
		}

		mapColnameToIndex04 := columnNameToIndex(&tableSchema04)
		str04, isAdded04 := buildQueryUpdate(tableID, &tableSchema04, &changeItem04, &mapColnameToIndex04)
		require.Equal(t, true, isAdded04)
		require.Equal(t, "UPDATE IGNORE `db`.`my_table_name_00` SET `a` = 1 WHERE `a` = 2 AND `b` = 2;", str04)

		// reverse order of tableSchema - to ensure that tested func works correctly with another order between changeItem & tableSchema
		tableSchema04 = reverseArr(tableSchema04)
		mapColnameToIndex04r := columnNameToIndex(&tableSchema04)
		str04r, isAdded04r := buildQueryUpdate(tableID, &tableSchema04, &changeItem04, &mapColnameToIndex04r)
		require.Equal(t, true, isAdded04r)
		require.Equal(t, "UPDATE IGNORE `db`.`my_table_name_00` SET `a` = 1 WHERE `a` = 2 AND `b` = 2;", str04r)
	})

	t.Run("two columns: both PrimaryKey, both changed", func(t *testing.T) {
		tableSchema05 := []abstract.ColSchema{
			{ColumnName: "a", PrimaryKey: true},
			{ColumnName: "b", PrimaryKey: true},
		}
		changeItem05 := abstract.ChangeItem{
			Kind:         "update",
			Schema:       "db",
			Table:        "my_table_name_00",
			ColumnNames:  []string{"a", "b"},
			ColumnValues: []interface{}{1, 2},
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"a", "b"},
				KeyValues: []interface{}{3, 4},
			},
		}

		tableID := abstract.TableID{
			Namespace: "db",
			Name:      "my_table_name_00",
		}

		mapColnameToIndex05 := columnNameToIndex(&tableSchema05)
		str05, isAdded05 := buildQueryUpdate(tableID, &tableSchema05, &changeItem05, &mapColnameToIndex05)
		require.Equal(t, true, isAdded05)
		require.Equal(t, "UPDATE IGNORE `db`.`my_table_name_00` SET `a` = 1, `b` = 2 WHERE `a` = 3 AND `b` = 4;", str05)

		// reverse order of tableSchema - to ensure that tested func works correctly with another order between changeItem & tableSchema
		tableSchema05 = reverseArr(tableSchema05)
		mapColnameToIndex05r := columnNameToIndex(&tableSchema05)
		str05r, isAdded05r := buildQueryUpdate(tableID, &tableSchema05, &changeItem05, &mapColnameToIndex05r)
		require.Equal(t, true, isAdded05r)
		require.Equal(t, "UPDATE IGNORE `db`.`my_table_name_00` SET `a` = 1, `b` = 2 WHERE `a` = 3 AND `b` = 4;", str05r)
	})
}

func Test_buildPartOfQueryInsert(t *testing.T) {
	t.Run("one column - PrimaryKey, changed", func(t *testing.T) {
		tableSchema00 := []abstract.ColSchema{{ColumnName: "a", PrimaryKey: true}}
		changeItem00 := abstract.ChangeItem{
			Kind:         "insert",
			ColumnNames:  []string{"a"},
			ColumnValues: []interface{}{1},
		}

		mapColnameToIndex00 := columnNameToIndex(&tableSchema00)
		require.Equal(t, "(1)", buildPartOfQueryInsert(&tableSchema00, &changeItem00, &mapColnameToIndex00))
	})
}

func Test_breakQueriesIntoBatches(t *testing.T) {
	require.Equal(t, [][]string{}, breakArrOfStringIntoBatchesByMaxSumLen([]string{}, 1))

	require.Equal(t, [][]string{{"a"}}, breakArrOfStringIntoBatchesByMaxSumLen([]string{"a"}, 1))
	require.Equal(t, [][]string{{"ab"}}, breakArrOfStringIntoBatchesByMaxSumLen([]string{"ab"}, 99999))
	require.Equal(t, [][]string{{"ab"}}, breakArrOfStringIntoBatchesByMaxSumLen([]string{"ab"}, 1)) // special case - check empty batch
	require.Equal(t, [][]string{{"a"}, {"ab"}, {"cd"}}, breakArrOfStringIntoBatchesByMaxSumLen([]string{"a", "ab", "cd"}, 1))
	require.Equal(t, [][]string{{"a", "b"}, {"cd"}}, breakArrOfStringIntoBatchesByMaxSumLen([]string{"a", "b", "cd"}, 2))

	require.Equal(t, [][]string{{"a"}, {"b"}}, breakArrOfStringIntoBatchesByMaxSumLen([]string{"a", "b"}, 1))

	require.Equal(t, [][]string{{"a", "b"}}, breakArrOfStringIntoBatchesByMaxSumLen([]string{"a", "b"}, 2))
	require.Equal(t, [][]string{{"a", "b"}}, breakArrOfStringIntoBatchesByMaxSumLen([]string{"a", "b"}, 2))
	require.Equal(t, [][]string{{"ab"}, {"c"}}, breakArrOfStringIntoBatchesByMaxSumLen([]string{"ab", "c"}, 2))

	require.Equal(t, [][]string{{"a", "b"}, {"c"}}, breakArrOfStringIntoBatchesByMaxSumLen([]string{"a", "b", "c"}, 2))
}

func Test_buildDeleteQueries(t *testing.T) {
	deleteConditions := []string{
		"(`a`=123 AND `b`=234)",
		"(`a`=123)",
		"(`a`=2 AND `b`=3)",
		"(`a`=2)",
	}
	tableID := abstract.TableID{
		Namespace: "db",
		Name:      "myTableName",
	}
	expectedDelete := []string{"DELETE FROM `db`.`myTableName` WHERE (`a`=123 AND `b`=234) OR (`a`=123) OR (`a`=2 AND `b`=3) OR (`a`=2);"}
	require.Equal(t, expectedDelete, buildDeleteQueries(tableID, deleteConditions))

	//-----------------------------------------------------------------------------------------------------------------

	var inputArr00 []string
	for i := 0; i < maxDeleteInBatch; i++ {
		inputArr00 = append(inputArr00, fmt.Sprintf("(`a`=%d)", i))
	}
	result00 := buildDeleteQueries(tableID, inputArr00)
	require.Equal(t, 1, len(result00))

	inputArr00 = append(inputArr00, "(`a`=%d)")

	result01 := buildDeleteQueries(tableID, inputArr00)
	require.Equal(t, 2, len(result01))
}

func Test_buildStatementOnDuplicateKeyUpdate(t *testing.T) {
	tableSchema00 := []abstract.ColSchema{
		{ColumnName: "a", PrimaryKey: true},
		{ColumnName: "b", PrimaryKey: true},
		{ColumnName: "c", PrimaryKey: true},
	}
	require.Equal(t, []string{}, buildStatementOnDuplicateKeyUpdate(&tableSchema00))

	tableSchema01 := []abstract.ColSchema{
		{ColumnName: "a", PrimaryKey: true},
		{ColumnName: "b", PrimaryKey: false},
	}
	require.Equal(t, []string{"`b` = VALUES(`b`)"}, buildStatementOnDuplicateKeyUpdate(&tableSchema01))

	tableSchema02 := []abstract.ColSchema{
		{ColumnName: "a", PrimaryKey: false},
		{ColumnName: "b", PrimaryKey: true},
	}
	require.Equal(t, []string{"`a` = VALUES(`a`)"}, buildStatementOnDuplicateKeyUpdate(&tableSchema02))

	tableSchema03 := []abstract.ColSchema{
		{ColumnName: "a", PrimaryKey: false},
		{ColumnName: "b", PrimaryKey: false},
	}
	require.Equal(t, []string{"`a` = VALUES(`a`)", "`b` = VALUES(`b`)"}, buildStatementOnDuplicateKeyUpdate(&tableSchema03))
}

func Test_setFqtn(t *testing.T) {
	sourceID := abstract.TableID{
		Namespace: "db",
		Name:      "entity",
	}
	destinationID := abstract.TableID{
		Namespace: "db1",
		Name:      "entity",
	}

	tableDDLWithoutDB := "CREATE TABLE IF NOT EXISTS `entity` (\n  `emp_no` int(11) NOT NULL,\n  `dept_no` char(4) NOT NULL,\n  `from_date` date NOT NULL,\n  `to_date` date NOT NULL,\n  PRIMARY KEY (`emp_no`,`dept_no`),\n  KEY `dept_no` (`dept_no`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	fixedTableDDL := setFqtn(tableDDLWithoutDB, sourceID, destinationID, true)
	require.Equal(
		t,
		"CREATE TABLE IF NOT EXISTS `db1`.`entity` (\n  `emp_no` int(11) NOT NULL,\n  `dept_no` char(4) NOT NULL,\n  `from_date` date NOT NULL,\n  `to_date` date NOT NULL,\n  PRIMARY KEY (`emp_no`,`dept_no`),\n  KEY `dept_no` (`dept_no`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
		fixedTableDDL,
	)

	tableDDLWithDB := "CREATE TABLE IF NOT EXISTS `db`.`entity` (\n  `emp_no` int(11) NOT NULL,\n  `dept_no` char(4) NOT NULL,\n  `from_date` date NOT NULL,\n  `to_date` date NOT NULL,\n  PRIMARY KEY (`emp_no`,`dept_no`),\n  KEY `dept_no` (`dept_no`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	fixedTableDDL = setFqtn(tableDDLWithDB, sourceID, destinationID, true)
	require.Equal(
		t,
		"CREATE TABLE IF NOT EXISTS `db1`.`entity` (\n  `emp_no` int(11) NOT NULL,\n  `dept_no` char(4) NOT NULL,\n  `from_date` date NOT NULL,\n  `to_date` date NOT NULL,\n  PRIMARY KEY (`emp_no`,`dept_no`),\n  KEY `dept_no` (`dept_no`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
		fixedTableDDL,
	)

	viewDDLWithoutDB := "CREATE OR REPLACE ALGORITHM=UNDEFINED VIEW `entity` (`sys_version`,`mysql_version`) AS select '2.1.1' AS `sys_version`,version() AS `mysql_version`"
	fixedViewDDL := setFqtn(viewDDLWithoutDB, sourceID, destinationID, true)
	require.Equal(
		t,
		"CREATE OR REPLACE ALGORITHM=UNDEFINED VIEW `db1`.`entity` (`sys_version`,`mysql_version`) AS select '2.1.1' AS `sys_version`,version() AS `mysql_version`;",
		fixedViewDDL,
	)

	viewDDLWithDB := "CREATE OR REPLACE ALGORITHM=UNDEFINED VIEW `db`.`entity` (`sys_version`,`mysql_version`) AS select '2.1.1' AS `sys_version`,version() AS `mysql_version`;"
	fixedViewDDL = setFqtn(viewDDLWithDB, sourceID, destinationID, true)
	require.Equal(
		t,
		"CREATE OR REPLACE ALGORITHM=UNDEFINED VIEW `db1`.`entity` (`sys_version`,`mysql_version`) AS select '2.1.1' AS `sys_version`,version() AS `mysql_version`;",
		fixedViewDDL,
	)

	functionDDLWithoutDB := "drop FUNCTION if exists `entity`;\n" +
		`CREATE FUNCTION entity() RETURNS tinyint unsigned
    NO SQL
    COMMENT '123'
BEGIN
    RETURN SUBSTRING_INDEX(SUBSTRING_INDEX(VERSION(), '-', 1), '.', 1);
END`
	fixedFunctionDDL := setFqtn(functionDDLWithoutDB, sourceID, destinationID, true)
	require.Equal(
		t,
		"drop FUNCTION if exists `db1`.`entity`;\nCREATE FUNCTION `db1`.`entity`() RETURNS tinyint unsigned\n"+
			`    NO SQL
    COMMENT '123'
BEGIN
    RETURN SUBSTRING_INDEX(SUBSTRING_INDEX(VERSION(), '-', 1), '.', 1);
END;`,
		fixedFunctionDDL,
	)

	functionDDLWithDB := "drop FUNCTION if exists `db`.`entity`;\n" +
		`CREATE FUNCTION db.entity() RETURNS tinyint unsigned
    NO SQL
    COMMENT '123'
BEGIN
    RETURN SUBSTRING_INDEX(SUBSTRING_INDEX(VERSION(), '-', 1), '.', 1);
END`
	fixedFunctionDDL = setFqtn(functionDDLWithDB, sourceID, destinationID, true)
	require.Equal(
		t,
		"drop FUNCTION if exists `db1`.`entity`;\nCREATE FUNCTION `db1`.`entity`() RETURNS tinyint unsigned\n"+
			`    NO SQL
    COMMENT '123'
BEGIN
    RETURN SUBSTRING_INDEX(SUBSTRING_INDEX(VERSION(), '-', 1), '.', 1);
END;`,
		fixedFunctionDDL,
	)

	triggerDDLWithoutDB := "drop trigger if exists `entity`;\n" +
		"CREATE TRIGGER `entity` BEFORE INSERT ON `customers` FOR EACH ROW SET NEW.`creditLimit` = NEW.`creditLimit` * 1000"
	fixedTriggerDDL := setFqtn(triggerDDLWithoutDB, sourceID, destinationID, true)
	require.Equal(
		t,
		"drop trigger if exists `db1`.`entity`;\n"+
			"CREATE TRIGGER `db1`.`entity` BEFORE INSERT ON `customers` FOR EACH ROW SET NEW.`creditLimit` = NEW.`creditLimit` * 1000;",
		fixedTriggerDDL,
	)

	triggerDDLWithDB := "drop trigger if exists `db`.`entity`;\n" +
		"CREATE TRIGGER `db`.`entity` BEFORE INSERT ON `customers` FOR EACH ROW SET NEW.`creditLimit` = NEW.`creditLimit` * 1000"
	fixedTriggerDDL = setFqtn(triggerDDLWithDB, sourceID, destinationID, true)
	require.Equal(
		t,
		"drop trigger if exists `db1`.`entity`;\n"+
			"CREATE TRIGGER `db1`.`entity` BEFORE INSERT ON `customers` FOR EACH ROW SET NEW.`creditLimit` = NEW.`creditLimit` * 1000;",
		fixedTriggerDDL,
	)
	tableNameIn := "CREATE TRIGGER entity AFTER INSERT ON internal_content\n  FOR EACH ROW BEGIN\n    DECLARE CONTENTID BIGINT;\n    DECLARE CONTENTNAME VARCHAR(255);\n    SET CONTENTID = NEW.`id`;\n    SET CONTENTNAME = NEW.`name`;\n    CALL procedure_entity(CONTENTID, CONCAT(CONTENTNAME, ' '));\n END"
	fixedTableDDL = setFqtn(tableNameIn, sourceID, destinationID, true)
	require.Equal(
		t,
		"CREATE TRIGGER `db1`.`entity` AFTER INSERT ON internal_content\n  FOR EACH ROW BEGIN\n    DECLARE CONTENTID BIGINT;\n    DECLARE CONTENTNAME VARCHAR(255);\n    SET CONTENTID = NEW.`id`;\n    SET CONTENTNAME = NEW.`name`;\n    CALL procedure_entity(CONTENTID, CONCAT(CONTENTNAME, ' '));\n END;",
		fixedTableDDL,
	)
}
