//go:build !disable_mysql_provider

package mysql

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	event := RowsEvent{}
	event.Data = &replication.RowsEvent{}
	event.Data.Rows = [][]interface{}{}
	event.Data.Rows = append(event.Data.Rows, []interface{}{})
	event.Table = &schema.Table{}
	event.Table.Columns = append(event.Table.Columns, schema.TableColumn{Name: "TestColumn"})
	event.Table.Columns = append(event.Table.Columns, schema.TableColumn{Name: "ColumnTest"})
	err := Validate(&event)
	require.Error(t, err)
}
