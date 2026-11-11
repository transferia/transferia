package transfer_state

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
)

// lostTableStateReply stores the request but loses its acknowledgement.
type lostTableStateReply struct {
	*serialState
	writes int
}

func newLostTableStateReply() *lostTableStateReply {
	return &lostTableStateReply{serialState: newSerialState(), writes: 0}
}

func (s *lostTableStateReply) SetTransferState(transferID string, values map[string]*coordinator.TransferStateData) error {
	s.writes++
	if err := s.serialState.SetTransferState(transferID, values); err != nil {
		return err
	}
	return errors.New("lost state acknowledgement")
}

func TestStoreTableRecords(test *testing.T) {
	ctx := context.Background()
	storage := newSerialState()
	store := NewState(storage, "transfer", "operation")
	table := store.NewTable(abstract.TableID{Name: "source"}, "destination", "DDL", false)
	sessions := map[string]string{"host": "session"}
	require.NoError(test, table.RegisterStaging(ctx, "__prefix_", sessions))
	sessions["host"] = "changed"
	restored, err := store.LoadTable(ctx, table.StateKey())
	require.NoError(test, err)
	require.Equal(test, "session", restored.StagingSessions()["host"])
	require.Len(test, storage.data, 1)
	var storedValue coordinator.TransferStateData
	require.NoError(test, json.Unmarshal(storage.data[table.StateKey()], &storedValue))
	require.NotContains(test, storedValue.Generic.(string), "StateKey")
	operations, err := store.ListTableStateKeysByOperation(ctx)
	require.NoError(test, err)
	require.Equal(test, map[string][]string{"operation": {table.StateKey()}}, operations)
	second := store.NewTable(abstract.TableID{Name: "second"}, "second", "DDL", false)
	require.NoError(test, second.RegisterStaging(ctx, "__second_", nil))
	keys, err := store.ListStateKeysWithPrefix(ctx, store.OperationStatePrefix())
	require.NoError(test, err)
	require.ElementsMatch(test, []string{table.StateKey(), second.StateKey()}, keys)
	require.NoError(test, store.RemoveTableState(ctx, second.StateKey()))
	restored, err = store.LoadTable(ctx, second.StateKey())
	require.NoError(test, err)
	require.Nil(test, restored)
}

func TestStoreStopsAfterLostWriteReply(test *testing.T) {
	ctx := context.Background()
	storage := newLostTableStateReply()
	store := NewState(storage, "transfer", "operation")
	table := store.NewTable(abstract.TableID{Name: "source"}, "destination", "DDL", false)
	require.Error(test, table.DecidePublication(ctx))
	require.True(test, table.StateWritesBlocked())
	require.Error(test, table.CompletePublication(ctx))
	require.Equal(test, 1, storage.writes)
	require.Error(test, store.RemoveTableState(ctx, table.StateKey()))
	recovered := NewState(storage, "transfer", "operation")
	restored, err := recovered.LoadTable(ctx, table.StateKey())
	require.NoError(test, err)
	require.True(test, restored.PublicationDecided())
	require.False(test, restored.PublicationComplete())
	// An ambiguous save blocks the operation, including its other tables.
	other := store.NewTable(abstract.TableID{Name: "other"}, "other", "DDL", false)
	require.Error(test, other.RegisterStaging(ctx, "__other_", nil))
	require.Equal(test, 1, storage.writes)
}

func TestStoreRejectsMismatchedIdentity(test *testing.T) {
	ctx := context.Background()
	storage := newSerialState()
	store := NewState(storage, "transfer", "operation")
	table := store.NewTable(abstract.TableID{Name: "source"}, "destination", "DDL", false)
	require.NoError(test, table.RegisterStaging(ctx, "__prefix_", nil))
	incorrectKey := store.TableStateKey(abstract.TableID{Name: "other"})
	storage.data[incorrectKey] = storage.data[table.StateKey()]
	restored, err := store.LoadTable(ctx, incorrectKey)
	require.ErrorContains(test, err, "state key mismatch")
	require.Nil(test, restored)
	emptyOperation := NewState(storage, "transfer", "").NewTable(abstract.TableID{Name: "source"}, "destination", "DDL", false)
	require.Error(test, emptyOperation.DecidePublication(ctx))
}

func TestTransferStateInventoryGroupsOnlyValidSnapshotKeys(test *testing.T) {
	storage := newSerialState()
	adapter := NewState(storage, "transfer", "operation")
	require.NoError(
		test,
		storage.SetTransferState(
			"transfer", map[string]*coordinator.TransferStateData{
				StatePrefix + "second/table/z": {},
				StatePrefix + "first/table/b":  {},
				StatePrefix + "first/table/a":  {},
				"other-provider/state":         {},
			},
		),
	)
	groups, err := adapter.ListTableStateKeysByOperation(context.Background())
	require.NoError(test, err)
	require.Equal(
		test,
		map[string][]string{
			"first":  {StatePrefix + "first/table/a", StatePrefix + "first/table/b"},
			"second": {StatePrefix + "second/table/z"},
		},
		groups,
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	groups, err = adapter.ListTableStateKeysByOperation(ctx)
	require.ErrorIs(test, err, context.Canceled)
	require.Nil(test, groups)

	for _, key := range []string{StatePrefix + "/table/a", StatePrefix + "first/attempt/a", StatePrefix + "first/table/", StatePrefix + "first/table/a/extra"} {
		require.NoError(test, storage.SetTransferState("transfer", map[string]*coordinator.TransferStateData{key: {}}))
		groups, err = adapter.ListTableStateKeysByOperation(context.Background())
		require.ErrorContains(test, err, "unexpected snapshot table state key")
		require.Nil(test, groups)
		require.NoError(test, storage.RemoveTransferState("transfer", []string{key}))
	}
}

// serialState models a remote JSON store, rather than retaining pointers into
// the running sink. Failed state writes must not leak through pointer aliases.
type serialState struct {
	coordinator.TransferState
	data map[string]json.RawMessage
}

func newSerialState() *serialState {
	return &serialState{
		TransferState: nil,
		data:          make(map[string]json.RawMessage),
	}
}

func (s *serialState) GetTransferStateKeys(string) ([]string, error) {
	var result []string
	for key := range s.data {
		result = append(result, key)
	}
	return result, nil
}

func (s *serialState) GetTransferStateByKeys(_ string, keys []string) (map[string]*coordinator.TransferStateData, error) {
	result := make(map[string]*coordinator.TransferStateData)
	for _, key := range keys {
		if raw, ok := s.data[key]; ok {
			var value coordinator.TransferStateData
			if err := json.Unmarshal(raw, &value); err != nil {
				return nil, err
			}
			result[key] = &value
		}
	}
	return result, nil
}

func (s *serialState) SetTransferState(_ string, values map[string]*coordinator.TransferStateData) error {
	for key, value := range values {
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		s.data[key] = raw
	}
	return nil
}

func (s *serialState) RemoveTransferState(_ string, keys []string) error {
	for _, key := range keys {
		delete(s.data, key)
	}
	return nil
}

func TestStoreRejectsOldPublicationFormat(test *testing.T) {
	storage := newSerialState()
	store := NewState(storage, "transfer", "operation")
	key := store.TableStateKey(abstract.TableID{Name: "source"})
	require.NoError(test, storage.SetTransferState("transfer", map[string]*coordinator.TransferStateData{
		key: {Generic: `{"SourceTableID":{"Name":"source"},"PublishDecided":true}`},
	}))
	table, err := store.LoadTable(context.Background(), key)
	require.ErrorContains(test, err, "unsupported snapshot table state format")
	require.Nil(test, table)
	require.Len(test, storage.data, 1)
}
