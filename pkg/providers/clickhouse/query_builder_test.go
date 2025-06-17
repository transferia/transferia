//go:build !disable_clickhouse_provider

package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

func TestSimplest(t *testing.T) {
	query := buildSelectQuery(&abstract.TableDescription{Name: "myTableName", Schema: "mySchema", Filter: "", Offset: 0}, abstract.TableColumns{
		{ColumnName: "columnA", Expression: "", OriginalType: "ch:UInt64"},
		{ColumnName: "columnB", Expression: "", OriginalType: "ch:String"},
	}, false, false, "")
	require.Equal(t, "SELECT `columnA`,`columnB` FROM `mySchema`.`myTableName`  WHERE 1=1 ", query)
}

func TestDecimal(t *testing.T) {
	query := buildSelectQuery(&abstract.TableDescription{Name: "myTableName", Schema: "mySchema", Filter: "", Offset: 0}, abstract.TableColumns{
		{ColumnName: "columnA", Expression: "", OriginalType: "ch:UInt64"},
		{ColumnName: "columnB", Expression: "", OriginalType: "ch:Decimal(1,2)"},
	}, false, false, "")
	require.Equal(t, "SELECT `columnA`,toString(`columnB`) FROM `mySchema`.`myTableName`  WHERE 1=1 ", query)
}

func TestVirtualColsHomoTransfer(t *testing.T) {
	query1 := buildSelectQuery(&abstract.TableDescription{Name: "myTableName", Schema: "mySchema", Filter: "", Offset: 0}, abstract.TableColumns{
		{ColumnName: "columnA", Expression: "", OriginalType: "ch:UInt64"},
		{ColumnName: "columnB", Expression: "ALIAS:blablabla", OriginalType: "ch:String"},
	}, true, false, "")
	require.Equal(t, "SELECT `columnA` FROM `mySchema`.`myTableName`  WHERE 1=1 ", query1)

	query2 := buildSelectQuery(&abstract.TableDescription{Name: "myTableName", Schema: "mySchema", Filter: "", Offset: 0}, abstract.TableColumns{
		{ColumnName: "columnA", Expression: "", OriginalType: "ch:UInt64"},
		{ColumnName: "columnB", Expression: "MATERIALIZED:blablabla", OriginalType: "ch:String"},
	}, true, false, "")
	require.Equal(t, "SELECT `columnA` FROM `mySchema`.`myTableName`  WHERE 1=1 ", query2)
}

func TestVirtualColsNotHomoTransfer(t *testing.T) {
	query1 := buildSelectQuery(&abstract.TableDescription{Name: "myTableName", Schema: "mySchema", Filter: "", Offset: 0}, abstract.TableColumns{
		{ColumnName: "columnA", Expression: "", OriginalType: "ch:UInt64"},
		{ColumnName: "columnB", Expression: "ALIAS:blablabla", OriginalType: "ch:String"},
	}, false, false, "")
	require.Equal(t, "SELECT `columnA`,`columnB` FROM `mySchema`.`myTableName`  WHERE 1=1 ", query1)

	query2 := buildSelectQuery(&abstract.TableDescription{Name: "myTableName", Schema: "mySchema", Filter: "", Offset: 0}, abstract.TableColumns{
		{ColumnName: "columnA", Expression: "", OriginalType: "ch:UInt64"},
		{ColumnName: "columnB", Expression: "MATERIALIZED:blablabla", OriginalType: "ch:String"},
	}, false, false, "")
	require.Equal(t, "SELECT `columnA`,`columnB` FROM `mySchema`.`myTableName`  WHERE 1=1 ", query2)
}

func TestReplacingMergeTree(t *testing.T) {
	query := buildSelectQuery(&abstract.TableDescription{Name: "myTableName", Schema: "mySchema", Filter: "", Offset: 0}, abstract.TableColumns{
		{ColumnName: "columnA", Expression: "", OriginalType: "ch:UInt64"},
		{ColumnName: "columnB", Expression: "", OriginalType: "ch:String"},
	}, false, false, "")
	require.Equal(t, "SELECT `columnA`,`columnB` FROM `mySchema`.`myTableName`  WHERE 1=1 ", query)
}

func TestUpdateableStorage(t *testing.T) {
	query := buildSelectQuery(&abstract.TableDescription{Name: "myTableName", Schema: "mySchema", Filter: "", Offset: 0}, abstract.TableColumns{
		{ColumnName: "columnA", Expression: "", OriginalType: "ch:UInt64"},
		{ColumnName: "columnB", Expression: "", OriginalType: "ch:String"},
	}, false, true, "")
	require.Equal(t, "SELECT `columnA`,`columnB` FROM `mySchema`.`myTableName`  FINAL  WHERE 1=1  AND __data_transfer_delete_time==0", query)
}

func TestSimplestWithFilter(t *testing.T) {
	query := buildSelectQuery(&abstract.TableDescription{Name: "myTableName", Schema: "mySchema", Filter: "columnA>33", Offset: 0}, abstract.TableColumns{
		{ColumnName: "columnA", Expression: "", OriginalType: "ch:UInt64"},
		{ColumnName: "columnB", Expression: "", OriginalType: "ch:String"},
	}, false, false, "")
	require.Equal(t, "SELECT `columnA`,`columnB` FROM `mySchema`.`myTableName`  WHERE 1=1  AND (columnA>33)", query)
}

func TestSimplestWithOffset(t *testing.T) {
	query := buildSelectQuery(&abstract.TableDescription{Name: "myTableName", Schema: "mySchema", Filter: "", Offset: 1000}, abstract.TableColumns{
		{ColumnName: "columnA", Expression: "", OriginalType: "ch:UInt64", PrimaryKey: true},
		{ColumnName: "columnB", Expression: "", OriginalType: "ch:String"},
	}, false, false, "")
	require.Equal(t, "SELECT `columnA`,`columnB` FROM `mySchema`.`myTableName`  WHERE 1=1  ORDER BY columnA OFFSET 1000", query)
}

func TestSimplestWithFilterAndAdditionalExpr(t *testing.T) {
	query := buildSelectQuery(&abstract.TableDescription{Name: "myTableName", Schema: "mySchema", Filter: "columnA>33", Offset: 0}, abstract.TableColumns{
		{ColumnName: "columnA", Expression: "", OriginalType: "ch:UInt64"},
		{ColumnName: "columnB", Expression: "", OriginalType: "ch:String"},
	}, false, false, "columnA IN (1,2,3)")
	require.Equal(t, "SELECT `columnA`,`columnB` FROM `mySchema`.`myTableName`  WHERE 1=1  AND (columnA>33) AND (columnA IN (1,2,3))", query)
}
