package filter

import (
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/base"
)

var (
	_ ListableFilter        = new(TableDescriptionsFilter)
	_ base.DataObjectFilter = new(TableDescriptionsFilter)
)

type TableIDFilter struct {
	tables map[abstract.TableID]bool
}

func (f *TableIDFilter) Includes(obj base.DataObject) (bool, error) {
	if len(f.tables) == 0 {
		return true, nil
	}
	tID, err := obj.ToOldTableID()
	if err != nil {
		return false, xerrors.Errorf("error converting data object to TableID: %w", err)
	}
	return f.IncludesID(*tID)
}

func (f *TableIDFilter) IncludesID(tID abstract.TableID) (bool, error) {
	if len(f.tables) == 0 {
		return true, nil
	}
	return f.tables[tID], nil
}

func (f *TableIDFilter) ListTables() ([]abstract.TableID, error) {
	var res []abstract.TableID
	for k := range f.tables {
		res = append(res, k)
	}
	return res, nil
}

func NewFromTableIDs(tables []abstract.TableID) *TableIDFilter {
	tmap := map[abstract.TableID]bool{}
	for _, t := range tables {
		tmap[t] = true
	}
	return &TableIDFilter{tables: tmap}
}

func NewFromObjects(objects []string) (*TableIDFilter, error) {
	tmap := map[abstract.TableID]bool{}
	for _, t := range objects {
		tid, err := abstract.ParseTableID(t)
		if err != nil {
			return nil, xerrors.Errorf("unable to parse object: %w", err)
		}
		tmap[*tid] = true
	}
	return &TableIDFilter{tables: tmap}, nil
}
