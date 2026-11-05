package json

import (
	"github.com/transferia/transferia/pkg/abstract"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
)

func withJSONSystemColumns(schema *abstract.TableSchema, hideSystemCols bool) *abstract.TableSchema {
	if hideSystemCols {
		return schema
	}
	columns := schema.Columns()
	// An empty schema tells the resolver to try another file.
	if len(columns) == 0 {
		return schema
	}
	if len(columns) >= 2 && columns[0].ColumnName == s3_reader.FileNameSystemCol && columns[1].ColumnName == s3_reader.RowIndexSystemCol {
		return schema
	}
	return s3_reader.AppendSystemColsTableSchema(columns, !columns.HasPrimaryKey())
}

func wrapJSONParserReadFlushChunk(err reader_error.ReaderError) reader_error.ReaderError {
	return reader_error.WrapReaderError("json.parser.Read.FlushChunk", err)
}
