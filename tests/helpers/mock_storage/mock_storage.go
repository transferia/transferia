package mockstorage

import (
	"context"

	"github.com/transferia/transferia/pkg/abstract"
)

type MockStorage struct {
	TableSchemaF            func(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error)
	CloseF                  func()
	PingF                   func() error
	LoadTableF              func(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error
	TableListF              func(abstract.IncludeTableList) (abstract.TableMap, error)
	ExactTableRowsCountF    func(table abstract.TableID) (uint64, error)
	EstimateTableRowsCountF func(table abstract.TableID) (uint64, error)
	TableExistsF            func(table abstract.TableID) (bool, error)

	// sampleable
	TableSizeInBytesF    func(table abstract.TableID) (uint64, error)
	LoadTopBottomSampleF func(table abstract.TableDescription, pusher abstract.Pusher) error
	LoadRandomSampleF    func(table abstract.TableDescription, pusher abstract.Pusher) error
	LoadSampleBySetF     func(table abstract.TableDescription, keySet []map[string]interface{}, pusher abstract.Pusher) error
	TableAccessibleF     func(table abstract.TableDescription) bool
}

func (s *MockStorage) TableSchema(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
	return s.TableSchemaF(ctx, table)
}
func (s *MockStorage) Close() {
	s.CloseF()
}
func (s *MockStorage) Ping() error {
	return s.PingF()
}
func (s *MockStorage) LoadTable(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	return s.LoadTableF(ctx, table, pusher)
}
func (s *MockStorage) TableList(in abstract.IncludeTableList) (abstract.TableMap, error) {
	return s.TableListF(in)
}
func (s *MockStorage) ExactTableRowsCount(table abstract.TableID) (uint64, error) {
	return s.ExactTableRowsCountF(table)
}
func (s *MockStorage) EstimateTableRowsCount(table abstract.TableID) (uint64, error) {
	return s.EstimateTableRowsCountF(table)
}
func (s *MockStorage) TableExists(table abstract.TableID) (bool, error) {
	return s.TableExistsF(table)
}

// sampleable

func (s *MockStorage) TableSizeInBytes(table abstract.TableID) (uint64, error) {
	return s.TableSizeInBytesF(table)
}
func (s *MockStorage) LoadTopBottomSample(table abstract.TableDescription, pusher abstract.Pusher) error {
	return s.LoadTopBottomSampleF(table, pusher)
}
func (s *MockStorage) LoadRandomSample(table abstract.TableDescription, pusher abstract.Pusher) error {
	return s.LoadRandomSampleF(table, pusher)
}
func (s *MockStorage) LoadSampleBySet(table abstract.TableDescription, keySet []map[string]interface{}, pusher abstract.Pusher) error {
	return s.LoadSampleBySetF(table, keySet, pusher)
}
func (s *MockStorage) TableAccessible(table abstract.TableDescription) bool {
	return s.TableAccessibleF(table)
}

func NewMockStorage() *MockStorage {
	return &MockStorage{
		TableSchemaF:            func(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) { return nil, nil },
		CloseF:                  func() {},
		PingF:                   func() error { return nil },
		LoadTableF:              func(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error { return nil },
		TableListF:              func(abstract.IncludeTableList) (abstract.TableMap, error) { return nil, nil },
		ExactTableRowsCountF:    func(table abstract.TableID) (uint64, error) { return 0, nil },
		EstimateTableRowsCountF: func(table abstract.TableID) (uint64, error) { return 0, nil },
		TableExistsF:            func(table abstract.TableID) (bool, error) { return false, nil },
		TableSizeInBytesF:       func(table abstract.TableID) (uint64, error) { return 0, nil },
		LoadTopBottomSampleF:    func(table abstract.TableDescription, pusher abstract.Pusher) error { return nil },
		LoadRandomSampleF:       func(table abstract.TableDescription, pusher abstract.Pusher) error { return nil },
		LoadSampleBySetF: func(table abstract.TableDescription, keySet []map[string]interface{}, pusher abstract.Pusher) error {
			return nil
		},
		TableAccessibleF: func(table abstract.TableDescription) bool { return false },
	}
}
