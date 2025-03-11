package bufferer

import (
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

type mockSinker struct {
	Items      []abstract.ChangeItem
	ErrorBatch []error

	errorBatchI int
}

// NewMockSinker returns a mock of a sinker. It will return the given errors for each Push made to it.
// The users of this sinker can access items it collected directly.
func NewMockSinker(errors []error) *mockSinker {
	return &mockSinker{
		Items:       make([]abstract.ChangeItem, 0),
		ErrorBatch:  errors,
		errorBatchI: 0,
	}
}

func (m *mockSinker) Close() error {
	return m.nextError()
}

func (m *mockSinker) Push(input []abstract.ChangeItem) error {
	result := m.nextError()
	if result == nil {
		m.Items = append(m.Items, input...)
	}
	return result
}

func (m *mockSinker) nextError() error {
	var result error
	if m.errorBatchI < len(m.ErrorBatch) {
		result = m.ErrorBatch[m.errorBatchI]
		m.errorBatchI += 1
	}
	return result
}

func TestBuffererCommon(t *testing.T) {
	mock := NewMockSinker(nil)

	sinker := Bufferer(logger.Log, BuffererConfig{2, 0, 250 * time.Millisecond}, solomon.NewRegistry(nil))(mock)

	require.NoError(t, <-sinker.AsyncPush([]abstract.ChangeItem{{Kind: abstract.InitTableLoad}}))
	require.NoError(t, <-sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.InsertKind},
		{Kind: abstract.InsertKind},
		{Kind: abstract.InsertKind},
	}))
	require.Len(t, mock.Items, 4)

	require.NoError(t, <-sinker.AsyncPush([]abstract.ChangeItem{{Kind: abstract.DoneTableLoad}}))
	require.Len(t, mock.Items, 5)

	require.NoError(t, sinker.Close())
	require.Len(t, mock.Items, 5)
}

func TestBuffererPeriodicWithError(t *testing.T) {
	mock := NewMockSinker([]error{nil, nil, xerrors.New("woopsi doopsi")})

	sinker := Bufferer(logger.Log, BuffererConfig{5, 0, 250 * time.Millisecond}, solomon.NewRegistry(nil))(mock)

	require.NoError(t, <-sinker.AsyncPush([]abstract.ChangeItem{{Kind: abstract.InitTableLoad}}))

	require.NoError(t, <-sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.InsertKind},
		{Kind: abstract.InsertKind},
		{Kind: abstract.InsertKind},
	}))
	require.Len(t, mock.Items, 4)

	require.Error(t, <-sinker.AsyncPush([]abstract.ChangeItem{{Kind: abstract.DoneTableLoad}}))
	require.Len(t, mock.Items, 4)

	require.NoError(t, sinker.Close())
	require.Len(t, mock.Items, 4)
}

func TestBuffererWithInflight(t *testing.T) {
	mock := NewMockSinker(nil)

	sinker := Bufferer(logger.Log, BuffererConfig{10, 0, 2 * time.Second}, solomon.NewRegistry(nil))(mock)

	require.NoError(t, <-sinker.AsyncPush([]abstract.ChangeItem{{Kind: abstract.InitTableLoad}}))

	chs := make([]chan error, 0)
	chs = append(chs, sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.InsertKind},
		{Kind: abstract.InsertKind},
	}))
	chs = append(chs, sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.InsertKind},
		{Kind: abstract.InsertKind},
	}))
	chs = append(chs, sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.UpdateKind},
		{Kind: abstract.UpdateKind},
	}))
	chs = append(chs, sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.DeleteKind},
		{Kind: abstract.DeleteKind},
	}))
	require.NoError(t, <-chs[3])
	require.NoError(t, <-chs[0])
	require.NoError(t, <-chs[1])
	require.NoError(t, <-chs[2])
	require.Len(t, mock.Items, 9)

	require.NoError(t, <-sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.InsertKind},
		{Kind: abstract.InsertKind},
		{Kind: abstract.UpdateKind},
		{Kind: abstract.InsertKind},
		{Kind: abstract.InsertKind},
		{Kind: abstract.InsertKind},
		{Kind: abstract.InsertKind},
		{Kind: abstract.UpdateKind},
		{Kind: abstract.InsertKind},
		{Kind: abstract.DeleteKind},
	}))
	require.Len(t, mock.Items, 19)

	require.NoError(t, <-sinker.AsyncPush([]abstract.ChangeItem{{Kind: abstract.DoneTableLoad}}))
	require.Len(t, mock.Items, 20)

	require.NoError(t, sinker.Close())
	require.Len(t, mock.Items, 20)
}

func TestBuffererWithInflightErrorNoInit(t *testing.T) {
	mock := NewMockSinker([]error{nil, nil, xerrors.New("woopsi doopsi")})

	sinker := Bufferer(logger.Log, BuffererConfig{3, 0, 2 * time.Second}, solomon.NewRegistry(nil))(mock)

	chs := make([]chan error, 0)
	chs = append(chs, sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.InsertKind},
		{Kind: abstract.InsertKind},
	}))
	chs = append(chs, sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.UpdateKind},
		{Kind: abstract.UpdateKind},
	}))
	require.NoError(t, <-chs[0])
	require.NoError(t, <-chs[1])
	require.Len(t, mock.Items, 4)
	chs = make([]chan error, 0)

	chs = append(chs, sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.InsertKind},
	}))
	chs = append(chs, sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.InsertKind},
	}))
	chs = append(chs, sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.InsertKind},
	}))
	require.NoError(t, <-chs[0], "Possible cause: Requirement of a delay before the first flush was not met")
	require.NoError(t, <-chs[1])
	require.NoError(t, <-chs[2])
	require.Len(t, mock.Items, 7)
	chs = make([]chan error, 0)

	chs = append(chs, sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.InsertKind},
	}))
	chs = append(chs, sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.InsertKind},
	}))
	require.Error(t, <-chs[0])
	require.Error(t, <-chs[1])
	require.Len(t, mock.Items, 7)

	require.NoError(t, sinker.Close())
	require.Len(t, mock.Items, 7)
}

func TestBuffererWithIntervalOnly(t *testing.T) {
	mock := NewMockSinker(nil)

	sinker := Bufferer(logger.Log, BuffererConfig{0, 0, 250 * time.Millisecond}, solomon.NewRegistry(nil))(mock)

	require.NoError(t, <-sinker.AsyncPush([]abstract.ChangeItem{{Kind: abstract.InitTableLoad}}))
	require.NoError(t, <-sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.InsertKind},
		{Kind: abstract.InsertKind},
		{Kind: abstract.InsertKind},
	}))
	require.NoError(t, <-sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.UpdateKind},
		{Kind: abstract.UpdateKind},
		{Kind: abstract.UpdateKind},
	}))
	time.Sleep(time.Second)
	require.NoError(t, <-sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.UpdateKind},
		{Kind: abstract.DeleteKind},
		{Kind: abstract.InsertKind},
	}))
	require.Len(t, mock.Items, 10)

	require.NoError(t, <-sinker.AsyncPush([]abstract.ChangeItem{{Kind: abstract.DoneTableLoad}}))
	require.Len(t, mock.Items, 11)

	require.NoError(t, sinker.Close())
	require.Len(t, mock.Items, 11)
}

func TestBuffererWithCountOnly(t *testing.T) {
	mock := NewMockSinker(nil)

	sinker := Bufferer(logger.Log, BuffererConfig{2, 0, 0}, solomon.NewRegistry(nil))(mock)

	require.NoError(t, <-sinker.AsyncPush([]abstract.ChangeItem{{Kind: abstract.InitTableLoad}}))
	require.NoError(t, <-sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.InsertKind},
		{Kind: abstract.InsertKind},
	}))
	require.NoError(t, <-sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.UpdateKind},
		{Kind: abstract.UpdateKind},
		{Kind: abstract.UpdateKind},
	}))
	time.Sleep(time.Second)
	require.NoError(t, <-sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.InsertKind},
		{Kind: abstract.UpdateKind},
		{Kind: abstract.DeleteKind},
		{Kind: abstract.InsertKind},
	}))
	require.Len(t, mock.Items, 10)

	require.NoError(t, <-sinker.AsyncPush([]abstract.ChangeItem{{Kind: abstract.DoneTableLoad}}))
	require.Len(t, mock.Items, 11)

	require.NoError(t, sinker.Close())
	require.Len(t, mock.Items, 11)
}

func chIsEmpty(ch chan error) (bool, error) {
	select {
	case err := <-ch:
		return false, err
	default:
		return true, nil
	}
}

func TestBuffererWithSizeOnly(t *testing.T) {
	mock := NewMockSinker(nil)

	sinker := Bufferer(logger.Log, BuffererConfig{0, 2 * humanize.MiByte, 0}, solomon.NewRegistry(nil))(mock)

	require.NoError(t, <-sinker.AsyncPush([]abstract.ChangeItem{{Kind: abstract.InitTableLoad}}))
	require.Len(t, mock.Items, 1)

	ch := sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.InsertKind, Size: abstract.EventSize{Values: 512 * humanize.KiByte}},
		{Kind: abstract.InsertKind, Size: abstract.EventSize{Values: 512 * humanize.KiByte}},
	})
	isEmpty, err := chIsEmpty(ch)
	require.NoError(t, err)
	require.True(t, true, isEmpty, "Items were pushed and not buffered")
	require.Len(t, mock.Items, 1)

	ch2 := sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.InsertKind, Size: abstract.EventSize{Values: 512 * humanize.KiByte}},
		{Kind: abstract.InsertKind, Size: abstract.EventSize{Values: 512 * humanize.KiByte}},
	})
	require.NoError(t, <-ch)
	require.NoError(t, <-ch2)
	require.Len(t, mock.Items, 5)

	ch = sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.InsertKind, Size: abstract.EventSize{Values: 256 * humanize.KiByte}},
		{Kind: abstract.InsertKind, Size: abstract.EventSize{Values: 256 * humanize.KiByte}},
	})
	isEmpty, err = chIsEmpty(ch)
	require.NoError(t, err)
	require.True(t, true, isEmpty, "Items were pushed and not buffered")
	require.Len(t, mock.Items, 5)

	require.NoError(t, sinker.Close())
	require.Len(t, mock.Items, 7)
}

func TestBuffererWithoutAnyTriggers(t *testing.T) {
	mock := NewMockSinker(nil)

	sinker := Bufferer(logger.Log, BuffererConfig{0, 0, 0}, solomon.NewRegistry(nil))(mock)

	require.NoError(t, <-sinker.AsyncPush([]abstract.ChangeItem{{Kind: abstract.InitTableLoad}}))
	require.Len(t, mock.Items, 1)

	chs := make([]chan error, 0)
	chs = append(chs, sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.InsertKind},
		{Kind: abstract.InsertKind},
	}))
	chs = append(chs, sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.UpdateKind},
		{Kind: abstract.UpdateKind},
		{Kind: abstract.UpdateKind},
	}))
	time.Sleep(time.Second)
	chs = append(chs, sinker.AsyncPush([]abstract.ChangeItem{
		{Kind: abstract.InsertKind},
		{Kind: abstract.UpdateKind},
		{Kind: abstract.DeleteKind},
		{Kind: abstract.InsertKind},
	}))
	require.Len(t, mock.Items, 1)

	require.NoError(t, sinker.Close())
	require.Len(t, mock.Items, 10)

	require.NoError(t, <-chs[0])
	require.NoError(t, <-chs[1])
	require.NoError(t, <-chs[2])
}
