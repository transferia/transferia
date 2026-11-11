package transfer_state

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
)

// in transfer_state data stored in format:
//     clickhouse_snapshot_v1/<operationID>/table/<murmur128(source TableID)>
// hash is calculated by:
//     murmur128(Namespace + "\x00" + Name)

// StatePrefix contains one publication and cleanup record per source table.
const StatePrefix = "clickhouse_snapshot_v1/"

type State struct {
	mutex               sync.Mutex
	transferStateClient coordinator.TransferState
	transferID          string
	operationID         string
	poisoned            error
}

func NewState(transferStateClient coordinator.TransferState, transferID string, operationID string) *State {
	return &State{
		mutex:               sync.Mutex{},
		transferStateClient: transferStateClient,
		transferID:          transferID,
		operationID:         operationID,
		poisoned:            nil,
	}
}

func (s *State) OperationStatePrefix() string {
	return StatePrefix + s.operationID + "/"
}

func (s *State) LoadTable(ctx context.Context, stateKey string) (*TableState, error) {
	if err := ctx.Err(); err != nil {
		return nil, xerrors.Errorf("load table state: %w", err)
	}
	values, err := s.transferStateClient.GetTransferStateByKeys(s.transferID, []string{stateKey})
	if err != nil {
		return nil, xerrors.Errorf("read snapshot table state: %w", err)
	}
	value, found := values[stateKey]
	if !found {
		return nil, nil
	}
	if value == nil {
		return nil, xerrors.New("snapshot table state is nil")
	}
	encoded, ok := value.Generic.(string)
	if !ok {
		return nil, xerrors.New("snapshot table state is not a JSON string")
	}
	var recordFields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(encoded), &recordFields); err != nil {
		return nil, xerrors.Errorf("decode snapshot table state: %w", err)
	}
	if _, found := recordFields["PublicationStatus"]; !found {
		return nil, xerrors.New("unsupported snapshot table state format; finish cleanup with the version that wrote this record")
	}
	var record ch_eo_common.SnapshotTable
	if err := json.Unmarshal([]byte(encoded), &record); err != nil {
		return nil, xerrors.Errorf("decode snapshot table state: %w", err)
	}
	if s.TableStateKey(record.SourceTableID) != stateKey {
		return nil, xerrors.New("snapshot table state key mismatch")
	}
	return newTableState(s, record), nil
}

func (s *State) saveRecord(ctx context.Context, tableState *ch_eo_common.SnapshotTable) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.poisoned != nil {
		return xerrors.Errorf("snapshot state updates are stopped: %w", s.poisoned)
	}
	if err := ctx.Err(); err != nil {
		return xerrors.Errorf("saveRecord: %w", err)
	}
	if s.operationID == "" {
		return xerrors.New("snapshot table state belongs to another operation")
	}
	stateJSON, err := json.Marshal(tableState)
	if err != nil {
		return xerrors.Errorf("encode snapshot table state: %w", err)
	}
	err = s.transferStateClient.SetTransferState(
		s.transferID,
		map[string]*coordinator.TransferStateData{
			s.TableStateKey(tableState.SourceTableID): {Generic: string(stateJSON)},
		},
	)
	if err != nil {
		s.poisoned = xerrors.Errorf("save snapshot table state; outcome is unknown: %w", err)
		return xerrors.Errorf("snapshot state updates are stopped: %w", s.poisoned)
	}
	return nil
}

func (s *State) ListStateKeysWithPrefix(ctx context.Context, stateKeyPrefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, xerrors.Errorf("ListStateKeysWithPrefix: %w", err)
	}
	stateKeys, err := s.transferStateClient.GetTransferStateKeys(s.transferID)
	if err != nil {
		return nil, xerrors.Errorf("list snapshot table state: %w", err)
	}
	matchingStateKeys := make([]string, 0)
	for _, stateKey := range stateKeys {
		if strings.HasPrefix(stateKey, stateKeyPrefix) {
			matchingStateKeys = append(matchingStateKeys, stateKey)
		}
	}
	sort.Strings(matchingStateKeys)
	return matchingStateKeys, nil
}

func (s *State) RemoveTableState(ctx context.Context, stateKey string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.poisoned != nil {
		return xerrors.Errorf("snapshot state updates are stopped: %w", s.poisoned)
	}
	if err := ctx.Err(); err != nil {
		return xerrors.Errorf("RemoveTableState: %w", err)
	}
	if !strings.HasPrefix(stateKey, s.OperationStatePrefix()+"table/") {
		return xerrors.New("snapshot table state belongs to another operation")
	}
	if err := s.transferStateClient.RemoveTransferState(s.transferID, []string{stateKey}); err != nil {
		return xerrors.Errorf("remove snapshot table state: %w", err)
	}
	return nil
}

// ListTableStateKeysByOperation inventories snapshot keys once; records are read separately
// so cleanup can reconcile state writes whose acknowledgements were lost.
func (s *State) ListTableStateKeysByOperation(ctx context.Context) (map[string][]string, error) {
	stateKeys, err := s.ListStateKeysWithPrefix(ctx, StatePrefix)
	if err != nil {
		return nil, xerrors.Errorf("ListTableStateKeysByOperation: %w", err)
	}
	tableStateKeysByOperation := make(map[string][]string)
	for _, stateKey := range stateKeys {
		keySegments := strings.Split(strings.TrimPrefix(stateKey, StatePrefix), "/")
		if len(keySegments) != 3 || keySegments[0] == "" || keySegments[1] != "table" || keySegments[2] == "" {
			return nil, xerrors.Errorf("unexpected snapshot table state key %q", stateKey)
		}
		operationID := keySegments[0]
		tableStateKeysByOperation[operationID] = append(tableStateKeysByOperation[operationID], stateKey)
	}
	return tableStateKeysByOperation, nil
}

func (s *State) OperationID() string { return s.operationID }

func (s *State) writesBlocked() bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.poisoned != nil
}

func (s *State) ForOperation(operationID string) *State {
	return NewState(s.transferStateClient, s.transferID, operationID)
}

func (s *State) TableStateKey(sourceTableID abstract.TableID) string {
	return s.OperationStatePrefix() + "table/" + ch_eo_common.Murmur128Hex(sourceTableID.Namespace+"\x00"+sourceTableID.Name)
}

func (s *State) NewTable(sourceTableID abstract.TableID, destinationTableName, ddlTemplate string, replicated bool) *TableState {
	return newTableState(s, *ch_eo_common.NewSnapshotTable(sourceTableID, destinationTableName, ddlTemplate, replicated))
}
