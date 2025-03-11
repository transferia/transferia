package model

import (
	"github.com/transferria/transferria/pkg/abstract"
)

type MockSource struct {
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
