package postgres

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

func skipChildrenIfCollapseInheritTables(collapseInheritTables bool, isChild bool) bool {
	return collapseInheritTables && isChild
}

func skipParentIfNotCollapseInheritTables(collapseInheritTables bool, isParent bool) bool {
	return (!collapseInheritTables) && isParent
}

func skipViewIfHomogeneous(isHomo bool, isView bool) bool {
	return isHomo && isView
}

func (s *Storage) Skipped(tableID abstract.TableID) (bool, error) {
	tinfo, err := s.getLoadTableMode(context.TODO(), abstract.TableDescription{
		Name:   tableID.Name,
		Schema: tableID.Namespace,
		Filter: "",
		EtaRow: 0,
		Offset: 0,
	})
	if err != nil {
		return false, xerrors.Errorf("unable to build table info: %w", err)
	}

	collapseInheritTables := s.collapseInheritTablesEnabled()
	return skipChildrenIfCollapseInheritTables(collapseInheritTables, tinfo.tableInfo.IsInherited) ||
		skipParentIfNotCollapseInheritTables(collapseInheritTables, tinfo.tableInfo.HasSubclass) ||
		skipViewIfHomogeneous(s.IsHomo, tinfo.tableInfo.IsView), nil
}
