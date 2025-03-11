package changeitem

import (
	"encoding/json"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util"
)

type TableSchema struct {
	columns TableColumns
	hash    string
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
	return MakeFastTableSchema(s.columns)
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
	}
}
