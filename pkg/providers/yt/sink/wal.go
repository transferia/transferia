//go:build !disable_yt_provider

package sink

import (
	"github.com/transferia/transferia/pkg/abstract"
)

var WalTableSchema = []abstract.ColSchema{
	{ColumnName: "id", DataType: "int64", PrimaryKey: true},
	{ColumnName: "nextlsn", DataType: "int64", PrimaryKey: true},
	{ColumnName: "txPosition", DataType: "int64", PrimaryKey: true},
	{ColumnName: "commitTime", DataType: "int64"},
	{ColumnName: "tx_id", DataType: "string"},
	{ColumnName: "kind", DataType: "string"},
	{ColumnName: "schema", DataType: "string"},
	{ColumnName: "table", DataType: "string"},
	{ColumnName: "columnnames", DataType: "any"},
	{ColumnName: "columnvalues", DataType: "any"},
	{ColumnName: "table_schema", DataType: "any"},
	{ColumnName: "oldkeys", DataType: "any"},
}
