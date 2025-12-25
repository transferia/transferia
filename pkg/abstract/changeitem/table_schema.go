package changeitem

import (
	"encoding/json"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util"
)

type TableSchema struct {
	tableID         TableID
	fastTableSchema FastTableSchema
	columns         TableColumns
	hash            string
}

func (s *TableSchema) TableID() TableID {
	if s.tableID.Name == "" && len(s.columns) != 0 {
		return s.columns[0].TableID()
	}
	return s.tableID
}

func (s *TableSchema) SetTableID(TableID TableID) {
	s.tableID = TableID
}

func (s *TableSchema) Copy() *TableSchema {
	if s == nil {
		return nil
	}
	return NewTableSchema(s.columns.Copy())
}

func (s *TableSchema) Columns() TableColumns {
	if s == nil {
		return nil
	}
	return s.columns
}

func (s *TableSchema) ColumnNames() []string {
	return s.columns.ColumnNames()
}

func (s *TableSchema) FastColumns() FastTableSchema {
	if s.fastTableSchema != nil {
		return s.fastTableSchema
	}
	s.fastTableSchema = MakeFastTableSchema(s.columns)
	return s.fastTableSchema
}

func (s *TableSchema) Hash() (string, error) {
	if s == nil || len(s.columns) == 0 {
		return "", xerrors.New("empty schema")
	}

	if len(s.hash) == 0 {
		serializedColumns, err := json.Marshal(s.columns)
		if err != nil {
			return "", xerrors.Errorf("cannot serialize schema: %w", err)
		}
		s.hash = util.HashSha256(serializedColumns)
	}
	return s.hash, nil
}

func (s *TableSchema) Equal(o *TableSchema) bool {
	sh, err := s.Hash()
	if err != nil {
		return false
	}
	oh, err := o.Hash()
	if err != nil {
		return false
	}
	return sh == oh
}

func NewTableSchema(columns []ColSchema) *TableSchema {
	return &TableSchema{
		columns: columns,
		hash:    "",
		tableID: TableID{
			Namespace: "",
			Name:      "",
		},
		fastTableSchema: nil,
	}
}
