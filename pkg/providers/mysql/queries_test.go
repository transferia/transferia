//go:build !disable_mysql_provider

package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestPrepareDDL(t *testing.T) {
	testCases := []struct {
		name        string
		tableID     abstract.TableID
		ts          *abstract.TableSchema
		expectedSQL string
	}{
		{
			name: "table with single primary key",
			tableID: abstract.TableID{
				Namespace: "test_schema",
				Name:      "test_table",
			},
			ts: abstract.NewTableSchema([]abstract.ColSchema{
				{
					ColumnName:   "id",
					OriginalType: "mysql:bigint",
					PrimaryKey:   true,
				},
				{
					ColumnName:   "name",
					OriginalType: "mysql:varchar",
					PrimaryKey:   false,
				},
			}),

			expectedSQL: "CREATE TABLE IF NOT EXISTS `test_schema`.`test_table` (\n`id` bigint ,\n`name` varchar ,\n  PRIMARY KEY (`id`) \n) ENGINE=InnoDB DEFAULT CHARSET=utf8;\n",
		},
		{
			name: "table with composite primary key",
			tableID: abstract.TableID{
				Namespace: "test_schema",
				Name:      "test_table",
			},
			ts: abstract.NewTableSchema([]abstract.ColSchema{
				{
					ColumnName:   "id1",
					OriginalType: "mysql:bigint",
					PrimaryKey:   true,
				},
				{
					ColumnName:   "id2",
					OriginalType: "mysql:bigint",
					PrimaryKey:   true,
				},
				{
					ColumnName:   "name",
					OriginalType: "mysql:varchar",
					PrimaryKey:   false,
				},
			}),

			expectedSQL: "CREATE TABLE IF NOT EXISTS `test_schema`.`test_table` (\n`id1` bigint ,\n`id2` bigint ,\n`name` varchar ,\n  PRIMARY KEY (`id1`,`id2`) \n) ENGINE=InnoDB DEFAULT CHARSET=utf8;\n",
		},
		{
			name: "table with no primary key",
			tableID: abstract.TableID{
				Namespace: "test_schema",
				Name:      "test_table",
			},
			ts: abstract.NewTableSchema([]abstract.ColSchema{
				{
					ColumnName:   "id",
					OriginalType: "mysql:bigint",
					PrimaryKey:   false,
				},
				{
					ColumnName:   "name",
					OriginalType: "mysql:varchar",
					PrimaryKey:   false,
				},
			}),

			expectedSQL: "CREATE TABLE IF NOT EXISTS `test_schema`.`test_table` (\n`id` bigint ,\n`name` varchar \n \n) ENGINE=InnoDB DEFAULT CHARSET=utf8;\n",
		},
		{
			name: "table with different column types",
			tableID: abstract.TableID{
				Namespace: "test_schema",
				Name:      "test_table",
			},
			ts: abstract.NewTableSchema([]abstract.ColSchema{
				{
					ColumnName:   "id",
					OriginalType: "mysql:bigint",
					PrimaryKey:   true,
				},
				{
					ColumnName:   "name",
					OriginalType: "mysql:varchar",
					PrimaryKey:   false,
				},
				{
					ColumnName:   "age",
					OriginalType: "mysql:integer",
					PrimaryKey:   false,
				},
				{
					ColumnName:   "salary",
					OriginalType: "mysql:numeric",
					PrimaryKey:   false,
				},
				{
					ColumnName:   "is_active",
					OriginalType: "mysql:boolean",
					PrimaryKey:   false,
				},
			}),

			expectedSQL: "CREATE TABLE IF NOT EXISTS `test_schema`.`test_table` (\n`id` bigint ,\n`name` varchar ,\n`age` integer ,\n`salary` numeric ,\n`is_active` boolean ,\n  PRIMARY KEY (`id`) \n) ENGINE=InnoDB DEFAULT CHARSET=utf8;\n",
		},
		{
			name: "table with special characters in names",
			tableID: abstract.TableID{
				Namespace: "test-schema",
				Name:      "test-table",
			},
			ts: abstract.NewTableSchema([]abstract.ColSchema{
				{
					ColumnName:   "user-id",
					OriginalType: "mysql:bigint",
					PrimaryKey:   true,
				},
				{
					ColumnName:   "full-name",
					OriginalType: "mysql:varchar",
					PrimaryKey:   false,
				},
			}),

			expectedSQL: "CREATE TABLE IF NOT EXISTS `test-schema`.`test-table` (\n`user-id` bigint ,\n`full-name` varchar ,\n  PRIMARY KEY (`user-id`) \n) ENGINE=InnoDB DEFAULT CHARSET=utf8;\n",
		},
		{
			name: "table with json type",
			tableID: abstract.TableID{
				Namespace: "test_schema",
				Name:      "test_table",
			},
			ts: abstract.NewTableSchema([]abstract.ColSchema{
				{
					ColumnName:   "id",
					OriginalType: "mysql:bigint",
					PrimaryKey:   true,
				},
				{
					ColumnName:   "data",
					OriginalType: "mysql:json",
					PrimaryKey:   false,
				},
			}),

			expectedSQL: "CREATE TABLE IF NOT EXISTS `test_schema`.`test_table` (\n`id` bigint ,\n`data` json ,\n  PRIMARY KEY (`id`) \n) ENGINE=InnoDB DEFAULT CHARSET=utf8;\n",
		},
		{
			name: "table with array type",
			tableID: abstract.TableID{
				Namespace: "test_schema",
				Name:      "test_table",
			},
			ts: abstract.NewTableSchema([]abstract.ColSchema{
				{
					ColumnName:   "id",
					OriginalType: "mysql:bigint",
					PrimaryKey:   true,
				},
				{
					ColumnName:   "tags",
					OriginalType: "mysql:varchar[]",
					PrimaryKey:   false,
				},
			}),

			expectedSQL: "CREATE TABLE IF NOT EXISTS `test_schema`.`test_table` (\n`id` bigint ,\n`tags` varchar[] ,\n  PRIMARY KEY (`id`) \n) ENGINE=InnoDB DEFAULT CHARSET=utf8;\n",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualQuery := prepareCreateTableQuery(tc.tableID, tc.ts)
			require.Equal(t, tc.expectedSQL, actualQuery)
		})
	}
}

func TestPrepareAlterTableQuery(t *testing.T) {
	tests := []struct {
		name          string
		tableID       abstract.TableID
		currentSchema *abstract.TableSchema
		newSchema     *abstract.TableSchema
		expectedQuery string
	}{
		{
			name: "Add single new column",
			tableID: abstract.TableID{
				Namespace: "test_schema",
				Name:      "test_table",
			},
			currentSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{
					ColumnName:   "id",
					OriginalType: "mysql:bigint",
					PrimaryKey:   true,
				},
				{
					ColumnName:   "name",
					OriginalType: "mysql:varchar",
					PrimaryKey:   false,
				},
			}),
			newSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{
					ColumnName:   "id",
					OriginalType: "mysql:bigint",
					PrimaryKey:   true,
				},
				{
					ColumnName:   "name",
					OriginalType: "mysql:varchar",
					PrimaryKey:   false,
				},
				{
					ColumnName:   "age",
					OriginalType: "mysql:integer",
					PrimaryKey:   false,
				},
			}),
			expectedQuery: "ALTER TABLE `test_schema`.`test_table` ADD COLUMN `age` integer;",
		},
		{
			name: "Add multiple new columns",
			tableID: abstract.TableID{
				Namespace: "test_schema",
				Name:      "test_table",
			},
			currentSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{
					ColumnName:   "id",
					OriginalType: "mysql:bigint",
					PrimaryKey:   true,
				},
			}),
			newSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{
					ColumnName:   "id",
					OriginalType: "mysql:bigint",
					PrimaryKey:   true,
				},
				{
					ColumnName:   "name",
					OriginalType: "mysql:varchar",
					PrimaryKey:   false,
				},
				{
					ColumnName:   "age",
					OriginalType: "mysql:integer",
					PrimaryKey:   false,
				},
				{
					ColumnName:   "is_active",
					OriginalType: "mysql:boolean",
					PrimaryKey:   false,
				},
			}),
			expectedQuery: "ALTER TABLE `test_schema`.`test_table` ADD COLUMN `name` varchar, ADD COLUMN `age` integer, ADD COLUMN `is_active` boolean;",
		},
		{
			name: "No new columns",
			tableID: abstract.TableID{
				Namespace: "test_schema",
				Name:      "test_table",
			},
			currentSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{
					ColumnName:   "id",
					OriginalType: "mysql:bigint",
					PrimaryKey:   true,
				},
				{
					ColumnName:   "name",
					OriginalType: "mysql:varchar",
					PrimaryKey:   false,
				},
			}),
			newSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{
					ColumnName:   "id",
					OriginalType: "mysql:bigint",
					PrimaryKey:   true,
				},
				{
					ColumnName:   "name",
					OriginalType: "mysql:varchar",
					PrimaryKey:   false,
				},
			}),
			expectedQuery: "",
		},
		{
			name: "Add JSON type column",
			tableID: abstract.TableID{
				Namespace: "test_schema",
				Name:      "test_table",
			},
			currentSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{
					ColumnName:   "id",
					OriginalType: "mysql:bigint",
					PrimaryKey:   true,
				},
			}),
			newSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{
					ColumnName:   "id",
					OriginalType: "mysql:bigint",
					PrimaryKey:   true,
				},
				{
					ColumnName:   "metadata",
					OriginalType: "mysql:json",
					PrimaryKey:   false,
				},
			}),
			expectedQuery: "ALTER TABLE `test_schema`.`test_table` ADD COLUMN `metadata` json;",
		},
		{
			name: "add columns from DataType",
			tableID: abstract.TableID{
				Namespace: "test_schema",
				Name:      "test_table",
			},
			currentSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{
					ColumnName: "id",
					DataType:   schema.TypeInt64.String(),
					PrimaryKey: true,
				},
			}),
			newSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{
					ColumnName: "id",
					DataType:   schema.TypeInt64.String(),
					PrimaryKey: true,
				},
				{
					ColumnName: "name",
					DataType:   schema.TypeString.String(),
					PrimaryKey: false,
				},
				{
					ColumnName: "metadata",
					DataType:   schema.TypeAny.String(),
					PrimaryKey: false,
				},
			}),
			expectedQuery: "ALTER TABLE `test_schema`.`test_table` ADD COLUMN `name` TEXT, ADD COLUMN `metadata` JSON;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := prepareAlterTableQuery(tt.tableID, tt.currentSchema, tt.newSchema)
			require.Equal(t, tt.expectedQuery, query)
		})
	}
}
