//go:build !disable_airbyte_provider

package airbyte

import (
	"encoding/binary"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/base"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"golang.org/x/exp/slices"
)

var RecordIndexCol = abstract.ColSchema{
	TableSchema:  "",
	TableName:    "",
	Path:         "",
	ColumnName:   "__dt_record_index",
	DataType:     ytschema.TypeInt64.String(),
	PrimaryKey:   true,
	FakeKey:      false,
	Required:     true,
	Expression:   "",
	OriginalType: "",
	Properties:   nil,
}

type RecordBatch struct {
	records        []Record
	iter           int
	size           int
	stream         *AirbyteStream
	colNames       []string
	tableSchema    *abstract.TableSchema
	colToIndex     map[string]int
	rowIndexOffset int
}

func (r *RecordBatch) AsChangeItems() ([]abstract.ChangeItem, error) {
	var res []abstract.ChangeItem
	for r.Next() {
		event := NewStreamRecord(
			r.stream,
			r.colNames,
			&r.records[r.iter],
			r.tableSchema,
			r.colToIndex,
			r.rowIndexOffset+r.iter,
		)
		ci, err := event.ToOldChangeItem()
		if err != nil {
			return nil, xerrors.Errorf("unable to construct change: %w", err)
		}
		res = append(res, *ci)
	}
	return res, nil
}

func (r *RecordBatch) Next() bool {
	r.iter++
	return len(r.records) > r.iter
}

func (r *RecordBatch) Count() int {
	return len(r.records)
}

func (r *RecordBatch) Size() int {
	return binary.Size(r.records)
}

func (r *RecordBatch) Event() (base.Event, error) {
	return NewStreamRecord(
		r.stream,
		r.colNames,
		&r.records[r.iter],
		r.tableSchema,
		r.colToIndex,
		r.rowIndexOffset+r.iter,
	), nil
}

func NewRecordBatch(rowIndexOffset int, stream *AirbyteStream) *RecordBatch {
	keys := map[string]bool{}
	for _, keyRow := range stream.SourceDefinedPrimaryKey {
		for _, colName := range keyRow {
			keys[colName] = true
		}
	}
	for _, cursor := range stream.DefaultCursorField {
		keys[cursor] = true
	}
	tableSchema := toSchema(stream.JSONSchema, keys)
	if len(stream.SourceDefinedPrimaryKey) == 0 {
		tableSchema = append(tableSchema, RecordIndexCol)
	}
	slices.SortFunc(tableSchema, func(a, b abstract.ColSchema) int {
		if a.IsKey() || b.IsKey() {
			return -1
		}
		return 1
	})
	var colNames []string
	colToIndex := map[string]int{}
	for i, col := range tableSchema {
		colNames = append(colNames, col.ColumnName)
		colToIndex[col.ColumnName] = i
	}
	return &RecordBatch{
		records:        make([]Record, 0),
		iter:           -1,
		size:           0,
		rowIndexOffset: rowIndexOffset,
		stream:         stream,
		colNames:       colNames,
		tableSchema:    abstract.NewTableSchema(tableSchema),
		colToIndex:     colToIndex,
	}
}
