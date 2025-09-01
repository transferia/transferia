package model

import (
	"github.com/transferia/transferia/pkg/abstract"
)

type MockSource struct {
	IsAbstract2Val   bool
	StorageFactory   func() abstract.Storage
	AllTablesFactory func() abstract.TableMap
}

var _ Source = (*MockSource)(nil)

func (s *MockSource) WithDefaults() {}

func (MockSource) IsSource() {}

func (MockSource) IsAsyncShardPartsSource() {}

func (s *MockSource) GetProviderType() abstract.ProviderType {
	return abstract.ProviderTypeMock
}

func (s *MockSource) GetName() string {
	return "mock_source"
}

func (s *MockSource) Validate() error {
	return nil
}

func (s *MockSource) IsAbstract2(Destination) bool {
	return s.IsAbstract2Val
}
