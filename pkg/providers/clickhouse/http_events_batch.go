//go:build !disable_clickhouse_provider

package clickhouse

import (
	"bufio"
	"bytes"
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/providers/clickhouse/format"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
)

type HTTPEventsBatch struct {
	Data        []byte
	Cols        *abstract.TableSchema
	scanner     *bufio.Scanner
	Part        *TablePartA2
	ColNames    []string
	readerStart time.Time
	Format      model.ClickhouseIOFormat
	RowCount    int
	SizeBytes   int
}

func (b *HTTPEventsBatch) ColumnNames() []string {
	var res []string
	for _, col := range b.Cols.Columns() {
		res = append(res, col.ColumnName)
	}
	return res
}

func (b *HTTPEventsBatch) Next() bool {
	return b.scanner.Scan()
}

func (b *HTTPEventsBatch) Count() int {
	return b.RowCount
}

func (b *HTTPEventsBatch) Size() int {
	return b.SizeBytes
}

func (b *HTTPEventsBatch) Event() (base.Event, error) {
	row := b.scanner.Bytes()
	return format.NewEvent(b.Format, row, b.Cols, b.ColNames, b.Part.TableID, b.readerStart)
}

func NewHTTPEventsBatch(part *TablePartA2, data []byte, cols *abstract.TableSchema, readerStart time.Time, format model.ClickhouseIOFormat, count int, size int) *HTTPEventsBatch {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	var colNames []string
	for _, col := range cols.Columns() {
		colNames = append(colNames, col.ColumnName)
	}
	return &HTTPEventsBatch{
		Data:        data,
		Cols:        cols,
		ColNames:    colNames,
		scanner:     scanner,
		Part:        part,
		readerStart: readerStart,
		Format:      format,
		RowCount:    count,
		SizeBytes:   size,
	}
}

func NewJSONCompactBatch(part *TablePartA2, data []byte, cols *abstract.TableSchema, readerStart time.Time, count int, size int) *HTTPEventsBatch {
	return NewHTTPEventsBatch(part, data, cols, readerStart, "JSONCompactEachRow", count, size)
}
