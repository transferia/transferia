package middlewares

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
)

type fakeSinker struct {
	t      *testing.T
	pusher func(item *abstract.ChangeItem) error
	move   func(ctx context.Context, src, dst abstract.TableID) error
}

func newFakeSinker(
	t *testing.T,
	pusher func(item *abstract.ChangeItem) error,
	move func(ctx context.Context, src, dst abstract.TableID) error,
) *fakeSinker {
	return &fakeSinker{t: t, pusher: pusher, move: move}
}

func (s *fakeSinker) Close() error {
	return nil
}

func (s *fakeSinker) Push(input []abstract.ChangeItem) error {
	for _, item := range input {
		err := s.pusher(&item)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *fakeSinker) Move(ctx context.Context, src, dst abstract.TableID) error {
	return s.move(ctx, src, dst)
}

func TestTmpPolicy(t *testing.T) {
	table := "table"
	transferID := "transfer"
	config := model.NewTmpPolicyConfig("_tmp", func(tableID abstract.TableID) bool {
		return tableID == *abstract.NewTableID("", table)
	})
	suffix := config.BuildSuffix(transferID)
	var hasSuffix bool
	sinker := TableTemporator(logger.Log, transferID, *config)(newFakeSinker(t,
		func(item *abstract.ChangeItem) error {
			require.Equal(t, hasSuffix, strings.HasSuffix(item.Table, suffix))
			require.Equal(t, table, strings.TrimSuffix(item.Table, suffix))
			return nil
		},
		func(ctx context.Context, src, dst abstract.TableID) error {
			require.Equal(t, table+suffix, src.Name)
			require.Equal(t, table, dst.Name)
			return nil
		}))

	hasSuffix = false
	require.NoError(t, sinker.Push([]abstract.ChangeItem{{Table: table, Kind: abstract.InsertKind}}))
	require.NoError(t, sinker.Push([]abstract.ChangeItem{{Table: table, Kind: abstract.UpdateKind}}))
	require.NoError(t, sinker.Push([]abstract.ChangeItem{{Table: table, Kind: abstract.DeleteKind}}))
	hasSuffix = true
	require.NoError(t, sinker.Push([]abstract.ChangeItem{{Table: table, Kind: abstract.InitTableLoad}}))
	require.NoError(t, sinker.Push([]abstract.ChangeItem{{Table: table, Kind: abstract.InsertKind}}))
	require.NoError(t, sinker.Push([]abstract.ChangeItem{{Table: table, Kind: abstract.UpdateKind}}))
	require.NoError(t, sinker.Push([]abstract.ChangeItem{{Table: table, Kind: abstract.DeleteKind}}))
	require.NoError(t, sinker.Push([]abstract.ChangeItem{{Table: table, Kind: abstract.DoneTableLoad}}))
	hasSuffix = false
	require.NoError(t, sinker.Push([]abstract.ChangeItem{{Table: table, Kind: abstract.InsertKind}}))
	require.NoError(t, sinker.Push([]abstract.ChangeItem{{Table: table, Kind: abstract.UpdateKind}}))
	require.NoError(t, sinker.Push([]abstract.ChangeItem{{Table: table, Kind: abstract.DeleteKind}}))
}

func TestTmpPolicyNotIncluded(t *testing.T) {
	table := "table"
	transferID := "transfer"
	config := model.NewTmpPolicyConfig("_tmp", nil)
	sinker := TableTemporator(logger.Log, transferID, *config)(newFakeSinker(t,
		func(item *abstract.ChangeItem) error {
			require.Equal(t, table, item.Table)
			return nil
		},
		func(ctx context.Context, src, dst abstract.TableID) error {
			require.Fail(t, "move must not be called for excluded table")
			return nil
		}))

	require.NoError(t, sinker.Push([]abstract.ChangeItem{{Table: table, Kind: abstract.InsertKind}}))
	require.NoError(t, sinker.Push([]abstract.ChangeItem{{Table: table, Kind: abstract.UpdateKind}}))
	require.NoError(t, sinker.Push([]abstract.ChangeItem{{Table: table, Kind: abstract.DeleteKind}}))

	require.NoError(t, sinker.Push([]abstract.ChangeItem{{Table: table, Kind: abstract.InitTableLoad}}))
	require.NoError(t, sinker.Push([]abstract.ChangeItem{{Table: table, Kind: abstract.InsertKind}}))
	require.NoError(t, sinker.Push([]abstract.ChangeItem{{Table: table, Kind: abstract.UpdateKind}}))
	require.NoError(t, sinker.Push([]abstract.ChangeItem{{Table: table, Kind: abstract.DeleteKind}}))
	require.NoError(t, sinker.Push([]abstract.ChangeItem{{Table: table, Kind: abstract.DoneTableLoad}}))

	require.NoError(t, sinker.Push([]abstract.ChangeItem{{Table: table, Kind: abstract.InsertKind}}))
	require.NoError(t, sinker.Push([]abstract.ChangeItem{{Table: table, Kind: abstract.UpdateKind}}))
	require.NoError(t, sinker.Push([]abstract.ChangeItem{{Table: table, Kind: abstract.DeleteKind}}))
}
