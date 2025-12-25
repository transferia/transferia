package mocksink

import "github.com/transferia/transferia/pkg/abstract"

type MockAsyncSink struct {
	PushCallback func(items []abstract.ChangeItem) error
}

func (s MockAsyncSink) AsyncPush(items []abstract.ChangeItem) chan error {
	errCh := make(chan error, 1)
	errCh <- s.PushCallback(items)
	return errCh
}

func (s MockAsyncSink) Close() error {
	return nil
}

func NewMockAsyncSink(callback func([]abstract.ChangeItem) error) *MockAsyncSink {
	if callback == nil {
		callback = func([]abstract.ChangeItem) error { return nil }
	}

	return &MockAsyncSink{
		PushCallback: callback,
	}
}
