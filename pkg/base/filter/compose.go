package filter

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/base"
)

type IntersectFilter struct {
	subFilters []base.DataObjectFilter
}

func (c *IntersectFilter) Includes(obj base.DataObject) (bool, error) {
	for i, f := range c.subFilters {
		ok, err := f.Includes(obj)
		if err != nil {
			return false, xerrors.Errorf("unable to check sub filter: %d: %T", i, f)
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

func (c *IntersectFilter) IncludesID(tID abstract.TableID) (bool, error) {
	for i, f := range c.subFilters {
		ok, err := f.IncludesID(tID)
		if err != nil {
			return false, xerrors.Errorf("unable to check sub filter: %d: %T", i, f)
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func (c *IntersectFilter) ListTables() ([]abstract.TableID, error) {
	tableSet := map[abstract.TableID]bool{}
	for _, f := range c.subFilters {
		if listable, ok := f.(ListableFilter); ok {
			tables, err := listable.ListTables()
			if err != nil {
				return nil, xerrors.Errorf("unable to list tables: %w", err)
			}
			for _, t := range tables {
				tableSet[t] = true
			}
		}
	}
	var res []abstract.TableID
	for t := range tableSet {
		res = append(res, t)
	}
	return res, nil
}

func (c *IntersectFilter) ListFilters() ([]abstract.TableDescription, error) {
	tableSet := map[abstract.TableID]abstract.TableDescription{}
	for _, f := range c.subFilters {
		if listable, ok := f.(FilterableFilter); ok {
			tables, err := listable.ListFilters()
			if err != nil {
				return nil, xerrors.Errorf("unable to list tables: %w", err)
			}
			for _, t := range tables {
				if _, ok := tableSet[t.ID()]; ok {
					return nil, xerrors.Errorf("multiple filter for %v", t.ID())
				}
				tableSet[t.ID()] = t
			}
		}
	}
	var res []abstract.TableDescription
	for _, t := range tableSet {
		res = append(res, t)
	}
	return res, nil
}

func NewIntersect(subFilters ...base.DataObjectFilter) *IntersectFilter {
	return &IntersectFilter{subFilters: subFilters}
}
