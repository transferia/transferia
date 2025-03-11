package filter

import (
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/base"
)

var (
	_ FilterableFilter      = new(TableDescriptionsFilter)
	_ ListableFilter        = new(TableDescriptionsFilter)
	_ base.DataObjectFilter = new(TableDescriptionsFilter)
)

type TableDescriptionsFilter struct {
	tables map[abstract.TableID]abstract.TableDescription
}

func (f TableDescriptionsFilter) Includes(obj base.DataObject) (bool, error) {
	if len(f.tables) == 0 {
		return true, nil
	}
	tID, err := obj.ToOldTableID()
	if err != nil {
		return false, xerrors.Errorf("error converting data object to TableID: %w", err)
	}
	return f.IncludesID(*tID)
}

func (f TableDescriptionsFilter) IncludesID(tID abstract.TableID) (bool, error) {
	if len(f.tables) == 0 {
		return true, nil
	}
	_, exist := f.tables[tID]
	return exist, nil
}

func (f TableDescriptionsFilter) ListTables() ([]abstract.TableID, error) {
	var res []abstract.TableID
	for k := range f.tables {
		res = append(res, k)
	}
	return res, nil
}

func (f TableDescriptionsFilter) ListFilters() ([]abstract.TableDescription, error) {
	var res []abstract.TableDescription
	for _, k := range f.tables {
		res = append(res, k)
	}
	return res, nil
}

func NewFromDescription(tables []abstract.TableDescription) *TableDescriptionsFilter {
	tmap := map[abstract.TableID]abstract.TableDescription{}
	for _, t := range tables {
		tmap[t.ID()] = t
	}
	return &TableDescriptionsFilter{tables: tmap}
}
