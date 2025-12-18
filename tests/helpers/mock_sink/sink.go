package mocksink

import "github.com/transferia/transferia/pkg/abstract"

type MockSink struct {
	PushCallback func([]abstract.ChangeItem) error
}

func (s *MockSink) Close() error {
	return nil
}

func (s *MockSink) Push(input []abstract.ChangeItem) error {
	return s.PushCallback(input)
}

func NewMockSink(callback func([]abstract.ChangeItem) error) *MockSink {
	return &MockSink{
		PushCallback: callback,
	}
}
