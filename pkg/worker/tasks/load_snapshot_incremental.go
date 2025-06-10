package tasks

import (
	"context"
	"slices"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/storage"
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

func (l *SnapshotLoader) getNextIncrementalState(ctx context.Context) ([]abstract.IncrementalState, error) {
	if !l.transfer.IsIncremental() {
		return nil, nil
	}
	srcStorage, err := storage.NewStorage(l.transfer, l.cp, l.registry)
	if err != nil {
		return nil, xerrors.Errorf(ResolveStorageErrorText, err)
	}
	if shardingContextStorage, ok := srcStorage.(abstract.ShardingContextStorage); ok && l.shardedState != "" {
		if err = shardingContextStorage.SetShardingContext([]byte(l.shardedState)); err != nil {
			return nil, errors.CategorizedErrorf(categories.Internal, "can't set sharded state to storage: %w", err)
		}
	}
	defer srcStorage.Close()
	incremental, ok := srcStorage.(abstract.IncrementalStorage)
	if !ok {
		return nil, nil
	}
	increment, err := incremental.GetNextIncrementalState(ctx, l.transfer.RegularSnapshot.Incremental)
	if err != nil {
		return nil, errors.CategorizedErrorf(categories.Internal, "unable to get incremental state: %w", err)
	}

	return increment, nil
}

func (l *SnapshotLoader) mergeWithNextIncrement(currentState []abstract.TableDescription, nextState []abstract.IncrementalState) ([]abstract.TableDescription, error) {
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

func (l *SnapshotLoader) getIncrementalState() ([]abstract.TableDescription, error) {
	if !l.transfer.IsIncremental() {
		return nil, nil
	}
	state, err := l.cp.GetTransferState(l.transfer.ID)
	if err != nil {
		return nil, errors.CategorizedErrorf(categories.Internal, "unable to get transfer state: %w", err)
	}
	logger.Log.Infof("get transfer(%s) state: %v", l.transfer.ID, state)
	relTables := state[TablesFilterStateKey].GetIncrementalTables()
	if relTables == nil {
		srcStorage, err := storage.NewStorage(l.transfer, l.cp, l.registry)
		if err != nil {
			return nil, xerrors.Errorf(ResolveStorageErrorText, err)
		}
		if shardingContextStorage, ok := srcStorage.(abstract.ShardingContextStorage); ok && l.shardedState != "" {
			if err = shardingContextStorage.SetShardingContext([]byte(l.shardedState)); err != nil {
				return nil, errors.CategorizedErrorf(categories.Internal, "can't set sharded state to storage: %w", err)
			}
		}
		incrementalStorage, ok := srcStorage.(abstract.IncrementalStorage)
		if !ok {
			return nil, nil
		}
		var tables []abstract.TableDescription
		for _, increment := range l.transfer.RegularSnapshot.Incremental {
			tables = append(tables, abstract.TableDescription{
				Name:   increment.Name,
				Schema: increment.Namespace,
				Filter: "",
				EtaRow: 0,
				Offset: 0,
			})
		}
		tablesOut := incrementalStorage.BuildArrTableDescriptionWithIncrementalState(tables, l.transfer.RegularSnapshot.Incremental)
		return tablesOut, nil
	}
	var res []abstract.TableDescription
	for _, tableState := range relTables {
		res = append(res, abstract.TableDescription{
			Name:   tableState.Name,
			Schema: tableState.Schema,
			Filter: tableState.Filter,
			EtaRow: 0,
			Offset: 0,
		})
	}
	return res, nil
}

func (l *SnapshotLoader) mergeWithIncrementalState(tables []abstract.TableDescription, incrementalStorage abstract.IncrementalStorage) ([]abstract.TableDescription, error) {
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
		result := incrementalStorage.BuildArrTableDescriptionWithIncrementalState(currTables, l.transfer.RegularSnapshot.Incremental)
		return result, nil
	}
	for i, table := range currTables {
		if table.Filter != "" || table.Offset != 0 {
			// table already contains predicate
			continue
		}
		for _, tableState := range relTables {
			stateID := tableState.ID()
			if table.ID() == stateID {
				currTables[i] = abstract.TableDescription{
					Name:   tableState.Name,
					Schema: tableState.Schema,
					Filter: tableState.Filter,
					EtaRow: 0,
					Offset: 0,
				}
			}
		}
	}
	result := incrementalStorage.BuildArrTableDescriptionWithIncrementalState(currTables, l.transfer.RegularSnapshot.Incremental)
	return result, nil
}
