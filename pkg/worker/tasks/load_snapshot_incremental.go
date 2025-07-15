package tasks

import (
	"context"
	"slices"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
)

const TablesFilterStateKey = "tables_filter"

func (l *SnapshotLoader) setIncrementalState(tableStates []abstract.IncrementalState) error {
	if len(tableStates) == 0 {
		return nil
	}
	err := l.cp.SetTransferState(
		l.transfer.ID,
		map[string]*coordinator.TransferStateData{
			TablesFilterStateKey: {
				IncrementalTables: abstract.IncrementalStateToTableDescription(tableStates),
			},
		},
	)
	if err != nil {
		return errors.CategorizedErrorf(categories.Internal, "unable to set transfer state: %w", err)
	}
	return nil
}

func (l *SnapshotLoader) getNextIncrementalState(ctx context.Context, inStorage abstract.Storage) ([]abstract.IncrementalState, error) {
	if !l.transfer.IsIncremental() {
		return nil, nil
	}
	incremental, ok := inStorage.(abstract.IncrementalStorage)
	if !ok {
		return nil, nil
	}
	increment, err := incremental.GetNextIncrementalState(ctx, l.transfer.RegularSnapshot.Incremental)
	if err != nil {
		return nil, errors.CategorizedErrorf(categories.Internal, "unable to get incremental state: %w", err)
	}

	return increment, nil
}

func (l *SnapshotLoader) mergeIncrementWithTables(currentState []abstract.TableDescription, nextState []abstract.IncrementalState) ([]abstract.TableDescription, error) {
	if !l.transfer.IsIncremental() {
		return currentState, nil
	}
	nextFilters := map[abstract.TableID]abstract.WhereStatement{}
	for _, nextTbl := range nextState {
		nextFilters[abstract.TableID{Namespace: nextTbl.Schema, Name: nextTbl.Name}] = nextTbl.Payload
	}
	for i, table := range currentState {
		if filter, ok := nextFilters[table.ID()]; ok && filter != abstract.NoFilter {
			currentState[i].Filter = abstract.FiltersIntersection(table.Filter, abstract.NotStatement(filter))
		}
	}
	return currentState, nil
}

func (l *SnapshotLoader) getIncrementalStateAndMergeWithTables(tables []abstract.TableDescription, incrementalStorage abstract.IncrementalStorage) ([]abstract.TableDescription, error) {
	currTables := slices.Clone(tables)
	if !l.transfer.CanReloadFromState() {
		logger.Log.Info("Transfer cannot load  snapshot from state!")
		return currTables, nil
	}

	logger.Log.Info("Transfer can load snapshot from state, calculating incremental state.")
	state, err := l.cp.GetTransferState(l.transfer.ID)
	if err != nil {
		return currTables, errors.CategorizedErrorf(categories.Internal, "unable to get transfer state: %w", err)
	}
	logger.Log.Infof("get transfer(%s) state: %v", l.transfer.ID, state)
	relTables := state[TablesFilterStateKey].GetIncrementalTables()
	if relTables == nil {
		logger.Log.Infof("Setting initial state %v", l.transfer.RegularSnapshot.Incremental)
		currTables2 := make([]abstract.TableDescription, 0)
		for _, increment := range l.transfer.RegularSnapshot.Incremental {
			currTables2 = append(currTables2, abstract.TableDescription{
				Name:   increment.Name,
				Schema: increment.Namespace,
				Filter: "",
				EtaRow: 0,
				Offset: 0,
			})
		}
		result := incrementalStorage.BuildArrTableDescriptionWithIncrementalState(currTables2, l.transfer.RegularSnapshot.Incremental)
		return result, nil
	}

	merge := func(in []abstract.TableDescription, newState []abstract.IncrementalState) []abstract.TableDescription {
		tmp := slices.Clone(in)
		myMap := make(map[abstract.TableID]*abstract.TableDescription)
		for _, el := range tmp {
			myMap[el.ID()] = &el
		}
		for _, el := range newState {
			if ptr, ok := myMap[el.ID()]; ok {
				if ptr.Filter != "" || ptr.Offset != 0 {
					// table already contains predicate
					continue
				}
				ptr.Filter = el.Payload
			} else {
				myMap[el.ID()] = &abstract.TableDescription{
					Schema: el.Schema,
					Name:   el.Name,
					Filter: el.Payload,
					EtaRow: uint64(0),
					Offset: uint64(0),
				}
			}
		}
		result := make([]abstract.TableDescription, 0, len(myMap))
		for _, v := range myMap {
			result = append(result, *v)
		}
		return result
	}

	tmp := merge(currTables, relTables)
	result := incrementalStorage.BuildArrTableDescriptionWithIncrementalState(tmp, l.transfer.RegularSnapshot.Incremental)
	return result, nil
}
