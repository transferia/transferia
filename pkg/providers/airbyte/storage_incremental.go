//go:build !disable_airbyte_provider

package airbyte

import (
	"context"
	"encoding/json"
	"slices"

	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ abstract.IncrementalStorage = (*Storage)(nil)

func (a *Storage) GetNextIncrementalState(ctx context.Context, incremental []abstract.IncrementalTable) ([]abstract.IncrementalState, error) {
	return yslices.Map(incremental, func(t abstract.IncrementalTable) abstract.IncrementalState {
		return abstract.IncrementalState{
			Name:    t.Name,
			Schema:  t.Namespace,
			Payload: "",
		}
	}), nil
}

// SetInitialState should have done nothing, since state handled inside loadTable method
func (a *Storage) BuildArrTableDescriptionWithIncrementalState(tables []abstract.TableDescription, incrementalTables []abstract.IncrementalTable) []abstract.TableDescription {
	result := slices.Clone(tables)
	for i, table := range result {
		for _, incremental := range incrementalTables {
			if incremental.CursorField == "" {
				continue
			}
			if table.ID() == incremental.TableID() {
				airbyteState, ok := a.state[StateKey(table.ID())]
				if ok && airbyteState != nil && airbyteState.Generic != nil {
					serialized, _ := json.Marshal(airbyteState.Generic)
					result[i] = abstract.TableDescription{
						Name:   incremental.Name,
						Schema: incremental.Namespace,
						Filter: abstract.WhereStatement(serialized),
						EtaRow: 0,
						Offset: 0,
					}
				}
			}
		}
	}
	a.logger.Info("incremental state synced", log.Any("tables", result), log.Any("state", a.state))
	return result
}
