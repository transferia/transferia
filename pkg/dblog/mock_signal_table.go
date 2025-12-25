package dblog

import (
	"context"

	"github.com/google/uuid"
	"github.com/transferia/transferia/pkg/abstract"
)

type MockSignalTable struct {
	CreateWatermarkF func(ctx context.Context, tableID abstract.TableID, watermarkType WatermarkType, lowBound []string) (uuid.UUID, error)
	IsWatermarkF     func(item *abstract.ChangeItem, tableID abstract.TableID, mark uuid.UUID) (bool, WatermarkType)
}

func (m *MockSignalTable) CreateWatermark(ctx context.Context, tableID abstract.TableID, watermarkType WatermarkType, lowBound []string) (uuid.UUID, error) {
	return m.CreateWatermarkF(ctx, tableID, watermarkType, lowBound)
}

func (m *MockSignalTable) IsWatermark(item *abstract.ChangeItem, tableID abstract.TableID, mark uuid.UUID) (bool, WatermarkType) {
	return m.IsWatermarkF(item, tableID, mark)
}

func NewMockSignalTable() *MockSignalTable {
	return &MockSignalTable{
		CreateWatermarkF: func(ctx context.Context, tableID abstract.TableID, watermarkType WatermarkType, lowBound []string) (uuid.UUID, error) {
			return uuid.UUID{}, nil
		},
		IsWatermarkF: func(item *abstract.ChangeItem, tableID abstract.TableID, mark uuid.UUID) (bool, WatermarkType) {
			return true, LowWatermarkType
		},
	}
}
