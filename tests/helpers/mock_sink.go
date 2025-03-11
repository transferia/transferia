package helpers

import "github.com/transferia/transferia/pkg/abstract"

type MockSink struct {
	PushCallback func([]abstract.ChangeItem)
}

func (s *MockSink) Close() error {
	return nil
}

func (s *MockSink) Push(input []abstract.ChangeItem) error {
	s.PushCallback(input)
	return nil
}
