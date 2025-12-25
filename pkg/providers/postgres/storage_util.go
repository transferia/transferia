package postgres

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

func isAnyChildPresent(foundTables abstract.TableMap, children []abstract.TableID) bool {
	for _, child := range children {
		if _, ok := foundTables[child]; ok {
			return true
		}
	}
	return false
}

func isAllChildrenPresent(foundTables abstract.TableMap, parentTableID abstract.TableID, parentToChildren map[abstract.TableID][]abstract.TableID) bool {
	children := parentToChildren[parentTableID]
	counter := 0
	for _, child := range children {
		if _, ok := foundTables[child]; ok {
			counter++
		}
	}
	return counter == len(parentToChildren[parentTableID])
}

func assertIfMatchedTableAndNotAllParts(foundTables abstract.TableMap, parentToChildren map[abstract.TableID][]abstract.TableID) error {
	for tableID := range foundTables {
		if children, ok := parentToChildren[tableID]; ok { // if it's a partitionedTable
			if isAnyChildPresent(foundTables, children) {
				if !isAllChildrenPresent(foundTables, tableID, parentToChildren) {
					return xerrors.Errorf("table %s is partitionedTable, and not all child tables matched", tableID.Fqtn())
				}
			}
		}
	}
	return nil
}

func makeParentToChildren(childToParent map[abstract.TableID]abstract.TableID) map[abstract.TableID][]abstract.TableID {
	parentToChildren := make(map[abstract.TableID][]abstract.TableID)
	for child, parent := range childToParent {
		parentToChildren[parent] = append(parentToChildren[parent], child)
	}
	return parentToChildren
}

func handlePartitionedTables(
	lgr log.Logger,
	foundTables abstract.TableMap,
	childToParent map[abstract.TableID]abstract.TableID,
) (abstract.TableMap, error) {
	parentToChildren := makeParentToChildren(childToParent)
	err := assertIfMatchedTableAndNotAllParts(foundTables, parentToChildren)
	if err != nil {
		return nil, xerrors.Errorf("check returned an error, err: %w", err)
	}

	result := map[abstract.TableID]abstract.TableInfo{}
	for tableID, tableInfo := range foundTables {
		if parentTableID, ok := childToParent[tableID]; ok { // if it's a part
			_, isFoundParentTableID := foundTables[parentTableID]
			if skipChildrenIfCollapseInheritTables(true, isFoundParentTableID) { // if it's child && parent present
				lgr.Infof("skip partition (bcs it can be included via parent table): %s", tableID.Fqtn())
				continue // throw out parts
			}
		}
		result[tableID] = tableInfo
	}
	return result, nil
}

func handleNotPartitionedTables(
	lgr log.Logger,
	foundTables abstract.TableMap,
	childToParent map[abstract.TableID]abstract.TableID,
) (abstract.TableMap, error) {
	parentToChildren := makeParentToChildren(childToParent)
	err := assertIfMatchedTableAndNotAllParts(foundTables, parentToChildren)
	if err != nil {
		return nil, xerrors.Errorf("check returned an error, err: %w", err)
	}

	result := map[abstract.TableID]abstract.TableInfo{}
	for tableID, tableInfo := range foundTables {
		_, childrenFound := parentToChildren[tableID]
		if skipParentIfNotCollapseInheritTables(false, childrenFound) {
			lgr.Infof("skip partitioned table (bcs all data in child tables): %s", tableID.Fqtn())
			continue // throw out parts
		}
		result[tableID] = tableInfo
	}
	return result, nil
}

func tableMapToArr(in abstract.TableMap) []tableIDWithInfo {
	result := make([]tableIDWithInfo, 0)
	for k, v := range in {
		result = append(result, tableIDWithInfo{ID: k, Info: v})
	}
	return result
}
