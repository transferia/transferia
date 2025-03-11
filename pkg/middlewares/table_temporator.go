package middlewares

import (
	"context"
	"sync"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"go.ytsaurus.tech/library/go/core/log"
)

// TableTemporator provides support for temporary policy
func TableTemporator(logger log.Logger, transferID string, config model.TmpPolicyConfig) func(abstract.Movable) abstract.Sinker {
	return func(m abstract.Movable) abstract.Sinker {
		return newTemporator(m, logger, transferID, config)
	}
}

type temporator struct {
	movable abstract.Movable

	config model.TmpPolicyConfig
	suffix string

	tablesDstTmp *tableMap

	logger log.Logger
}

func newTemporator(m abstract.Movable, logger log.Logger, transferID string, config model.TmpPolicyConfig) *temporator {
	return &temporator{
		movable: m,

		config: config,
		suffix: config.BuildSuffix(transferID),

		tablesDstTmp: newTableMap(),

		logger: logger,
	}
}

func (t *temporator) Close() error {
	// Close is never concurrent with Push, so iteration is safe
	for originalTableID, temporaryTableID := range t.tablesDstTmp.data {
		if err := t.moveFromTemporaryToDestination(temporaryTableID, originalTableID); err != nil {
			return xerrors.Errorf("failed to move from temporary %s to original %s: %w", temporaryTableID.Fqtn(), originalTableID.Fqtn(), err)
		}
	}
	return t.movable.Close()
}

func (t *temporator) Push(input []abstract.ChangeItem) error {
	begin := 0
	for i := range input {
		item := &input[i]
		originalTableID := item.TableID()

		if item.Kind == abstract.InitTableLoad && t.config.Include(originalTableID) {
			temporaryTableID := *abstract.NewTableID(item.Schema, item.Table+t.suffix)
			t.tablesDstTmp.Set(originalTableID, temporaryTableID)
			t.logger.Info("Temporary is used at sinker", log.String("table", originalTableID.Fqtn()), log.String("temporary_table", temporaryTableID.Fqtn()))
		}

		temporaryTableID, ok := t.tablesDstTmp.Get(originalTableID)
		if !ok {
			continue
		}

		item.SetTableID(temporaryTableID)

		if item.Kind == abstract.DoneTableLoad {
			end := i + 1
			inputPart := input[begin:end]
			if err := t.movable.Push(inputPart); err != nil {
				return xerrors.Errorf("failed to push input part [%d:%d]: %w", begin, end, err)
			}
			begin = end
			if err := t.moveFromTemporaryToDestination(temporaryTableID, originalTableID); err != nil {
				return xerrors.Errorf("failed to move from temporary %s to original %s: %w", temporaryTableID.Fqtn(), originalTableID.Fqtn(), err)
			}
		}
	}

	if begin >= len(input) {
		return nil
	}

	return t.movable.Push(input[begin:])
}

func (t *temporator) moveFromTemporaryToDestination(temporaryTableID abstract.TableID, originalTableID abstract.TableID) error {
	if err := t.movable.Move(context.TODO(), temporaryTableID, originalTableID); err != nil {
		return xerrors.Errorf("sinker.Move failed: %w", err)
	}
	t.tablesDstTmp.Delete(originalTableID)
	t.logger.Info("Successfully moved from temporary to destination", log.String("table", originalTableID.Fqtn()), log.String("temporary_table", temporaryTableID.Fqtn()))
	return nil
}

type tableMap struct {
	data  map[abstract.TableID]abstract.TableID
	mutex sync.RWMutex
}

func newTableMap() *tableMap {
	return &tableMap{
		data:  make(map[abstract.TableID]abstract.TableID),
		mutex: sync.RWMutex{},
	}
}

func (m *tableMap) Get(key abstract.TableID) (abstract.TableID, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	value, ok := m.data[key]
	return value, ok
}

func (m *tableMap) Set(key abstract.TableID, value abstract.TableID) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.data[key] = value
}

func (m *tableMap) Delete(key abstract.TableID) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.data, key)
}
