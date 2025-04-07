package airbyte

import (
	"context"
	"encoding/json"

	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ abstract.IncrementalStorage = (*Storage)(nil)

func (a *Storage) GetIncrementalState(ctx context.Context, incremental []abstract.IncrementalTable) ([]abstract.TableDescription, error) {
	return yslices.Map(incremental, func(t abstract.IncrementalTable) abstract.TableDescription {
		return abstract.TableDescription{
			Name:   t.Name,
			Schema: t.Namespace,
			Filter: "",
			EtaRow: 0,
			Offset: 0,
		}
	}), nil
}

// SetInitialState should have done nothing, since state handled inside loadTable method
func (a *Storage) SetInitialState(tables []abstract.TableDescription, incrementalTables []abstract.IncrementalTable) {
	for i, table := range tables {
		for _, incremental := range incrementalTables {
			if incremental.CursorField == "" {
				continue
			}
			if table.ID() == incremental.TableID() {
				airbyteState, ok := a.state[StateKey(table.ID())]
				if ok && airbyteState != nil && airbyteState.Generic != nil {
					serialized, _ := json.Marshal(airbyteState.Generic)
					tables[i] = abstract.TableDescription{
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
	a.logger.Info("incremental state synced", log.Any("tables", tables), log.Any("state", a.state))
}
