package table

import (
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

// YtTable is the passive-layer table abstraction. As with YtColumn it no longer
// embeds any active-plane interface; consumers use the accessors below plus
// AddColumn/ColumnNames/ToOldTable.
type YtTable interface {
	Database() string
	Schema() string
	Name() string
	FullName() string
	ColumnsCount() int
	Column(i int) YtColumn
	ColumnByName(name string) YtColumn
	ToOldTable() (*abstract.TableSchema, error)

	AddColumn(YtColumn)
	ColumnNames() ([]string, error)
}

type table struct {
	name             string
	columns          []YtColumn
	legacyTableCache *abstract.TableSchema
	colNameCache     []string
	cacheOnce        sync.Once
}

var _ YtTable = (*table)(nil)

func (t *table) Database() string {
	return ""
}

func (t *table) Schema() string {
	return ""
}

func (t *table) Name() string {
	return t.name
}

func (t *table) FullName() string {
	return t.name
}

func (t *table) ColumnsCount() int {
	return len(t.columns)
}

func (t *table) Column(i int) YtColumn {
	if i < 0 || i >= len(t.columns) {
		return nil
	}
	return t.columns[i]
}

func (t *table) ColumnByName(name string) YtColumn {
	for _, col := range t.columns {
		if col.Name() == name {
			return col
		}
	}
	return nil
}

func (t *table) ToOldTable() (*abstract.TableSchema, error) {
	if err := t.initCaches(); err != nil {
		return nil, xerrors.Errorf("error initializing OldTable cache: %w", err)
	}
	return t.legacyTableCache, nil
}

func (t *table) ColumnNames() ([]string, error) {
	if err := t.initCaches(); err != nil {
		return nil, xerrors.Errorf("error initializing column cache: %w", err)
	}
	return t.colNameCache, nil
}

func (t *table) AddColumn(col YtColumn) {
	col.setTable(t)
	t.columns = append(t.columns, col)
}

func (t *table) initCaches() error {
	var err error
	t.cacheOnce.Do(func() {
		t.colNameCache = make([]string, 0, len(t.columns))
		for _, col := range t.columns {
			t.colNameCache = append(t.colNameCache, col.Name())
		}

		tableCacheColumns := make([]abstract.ColSchema, 0, len(t.columns))
		for _, col := range t.columns {
			s, colErr := col.ToOldColumn()
			if colErr != nil {
				err = colErr
				return
			}
			tableCacheColumns = append(tableCacheColumns, *s)
		}
		t.legacyTableCache = abstract.NewTableSchema(tableCacheColumns)
	})
	return err
}

func NewTable(name string) YtTable {
	return &table{
		name:             name,
		columns:          nil,
		legacyTableCache: nil,
		colNameCache:     nil,
		cacheOnce:        sync.Once{},
	}
}
