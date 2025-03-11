package middlewares

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

type mockSinker struct {
	Items []abstract.ChangeItem
}

// NewMockSinker returns a mock of a sinker. It will return the given errors for each Push made to it.
// The users of this sinker can access items it collected directly.
func NewMockSinker() *mockSinker {
	return &mockSinker{
		Items: make([]abstract.ChangeItem, 0),
	}
}

func (m *mockSinker) Close() error {
	return nil
}

func (m *mockSinker) Push(input []abstract.ChangeItem) error {
	m.Items = append(m.Items, input...)
	return nil
}

func TestNonRowSeparatorSingleBatchOnlyRows(t *testing.T) {
	mock := NewMockSinker()

	sinker := NonRowSeparator()(mock)

	require.NoError(t, sinker.Push([]abstract.ChangeItem{
		{Kind: abstract.InsertKind},
		{Kind: abstract.InsertKind},
		{Kind: abstract.InsertKind},
		{Kind: abstract.UpdateKind},
		{Kind: abstract.DeleteKind},
		{Kind: abstract.InsertKind},
	}))
	require.Len(t, mock.Items, 6)

	require.NoError(t, sinker.Close())
	require.Len(t, mock.Items, 6)
}

func TestNonRowSeparatorTwoBatchesOnlyRows(t *testing.T) {
	mock := NewMockSinker()

	sinker := NonRowSeparator()(mock)

	require.NoError(t, sinker.Push([]abstract.ChangeItem{
		{Kind: abstract.InsertKind},
		{Kind: abstract.UpdateKind},
		{Kind: abstract.DeleteKind},
		{Kind: abstract.InsertKind},
	}))
	require.Len(t, mock.Items, 4)

	require.NoError(t, sinker.Push([]abstract.ChangeItem{
		{Kind: abstract.InsertKind},
	}))
	require.Len(t, mock.Items, 5)

	require.NoError(t, sinker.Close())
	require.Len(t, mock.Items, 5)
}

func TestNonRowSeparatorSingleBatchWithNonRows(t *testing.T) {
	mock := NewMockSinker()

	sinker := NonRowSeparator()(mock)

	require.NoError(t, sinker.Push([]abstract.ChangeItem{
		{Kind: abstract.InsertKind},
		{Kind: abstract.InsertKind},
		{Kind: abstract.UpdateKind},
		{Kind: abstract.DropTableKind},
		{Kind: abstract.DeleteKind},
		{Kind: abstract.InsertKind},
	}))
	require.Len(t, mock.Items, 6)

	require.NoError(t, sinker.Close())
	require.Len(t, mock.Items, 6)
}

func TestNonRowSeparatorTwoBatchesWithNonRows(t *testing.T) {
	mock := NewMockSinker()

	sinker := NonRowSeparator()(mock)

	require.NoError(t, sinker.Push([]abstract.ChangeItem{
		{Kind: abstract.DropTableKind},
	}))
	require.Len(t, mock.Items, 1)

	require.NoError(t, sinker.Push([]abstract.ChangeItem{
		{Kind: abstract.InsertKind},
		{Kind: abstract.DropTableKind},
	}))
	require.Len(t, mock.Items, 3)

	require.NoError(t, sinker.Close())
	require.Len(t, mock.Items, 3)
}

func TestNonRowSeparatorTwoBatchesWithoutRows(t *testing.T) {
	mock := NewMockSinker()

	sinker := NonRowSeparator()(mock)

	require.NoError(t, sinker.Push([]abstract.ChangeItem{}))
	require.Len(t, mock.Items, 0)

	require.NoError(t, sinker.Push([]abstract.ChangeItem{
		{Kind: abstract.DropTableKind},
	}))
	require.Len(t, mock.Items, 1)

	require.NoError(t, sinker.Close())
	require.Len(t, mock.Items, 1)
}

func TestNonRowSeparatorMultipleNonRows(t *testing.T) {
	mock := NewMockSinker()

	sinker := NonRowSeparator()(mock)

	require.NoError(t, sinker.Push([]abstract.ChangeItem{
		{Kind: abstract.DDLKind},
		{Kind: abstract.DDLKind},
		{Kind: abstract.DDLKind},
		{Kind: abstract.DDLKind},
	}))
	require.Len(t, mock.Items, 4)

	require.NoError(t, sinker.Close())
	require.Len(t, mock.Items, 4)
}
