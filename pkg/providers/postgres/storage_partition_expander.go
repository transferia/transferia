package postgres

import (
	"context"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

func (s *Storage) ExpandPartitions(ctx context.Context, tableMap abstract.TableMap) (abstract.TableMap, error) {
	if !s.IsHomo {
		return tableMap, nil
	}
	parentToChildren, err := s.getParentToChildMap(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to build child-parent map for partition expansion: %w", err)
	}
	parentsToExpand := findParentsToExpand(parentToChildren, tableMap)
	if len(parentsToExpand) == 0 {
		return tableMap, nil
	}

	conn, err := s.Conn.Acquire(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to acquire connection for partition expansion: %w", err)
	}
	defer conn.Release()

	for _, parentID := range parentsToExpand {
		// Collect all descendants (multi-level partitioning).
		descendants := collectAllDescendants(parentID, parentToChildren)
		for _, child := range descendants {
			if _, alreadyInMap := tableMap[child]; alreadyInMap {
				continue
			}
			etaRow, err := s.EstimateTableRowsCount(child)
			if err != nil {
				logger.Log.Warn("Unable to estimate table rows count", log.String("table", child.String()), log.Error(err))
			}
			childTable := abstract.TableDescription{
				Schema: child.Namespace,
				Name:   child.Name,
				Filter: "",
				EtaRow: etaRow,
				Offset: 0,
			}
			schema, err := s.LoadSchemaForTable(ctx, conn.Conn(), childTable)
			if err != nil {
				return nil, xerrors.Errorf("failed to load schema for partition child %s: %w", child.Fqtn(), err)
			}
			tableMap[child] = abstract.TableInfo{EtaRow: etaRow, IsView: false, Schema: schema}
			logger.Log.Infof("ExpandPartitions: added child %s of parent %s to upload list", child.Fqtn(), parentID.Fqtn())
		}
	}

	return tableMap, nil
}

func findParentsToExpand(parentToChildren map[abstract.TableID][]abstract.TableID, tableMap abstract.TableMap) []abstract.TableID {
	var toExpand []abstract.TableID
	for parentID, children := range parentToChildren {
		if _, parentIncluded := tableMap[parentID]; !parentIncluded {
			continue
		}
		anyChildIncluded := false
		for _, child := range children {
			if _, ok := tableMap[child]; ok {
				anyChildIncluded = true
				break
			}
		}
		if !anyChildIncluded {
			toExpand = append(toExpand, parentID) // If no children included – include parentID to expand list.
		}
	}
	return toExpand
}

// collectAllDescendants returns all descendants of a parent table (handles multi-level partitioning).
func collectAllDescendants(root abstract.TableID, parentToChildren map[abstract.TableID][]abstract.TableID) []abstract.TableID {
	var result []abstract.TableID
	queue := []abstract.TableID{root}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, child := range parentToChildren[current] {
			result = append(result, child)
			queue = append(queue, child)
		}
	}
	return result
}
