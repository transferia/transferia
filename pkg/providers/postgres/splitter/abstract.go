//go:build !disable_postgres_provider

package splitter

import (
	"context"
	"time"

	"github.com/transferia/transferia/pkg/abstract"
)

type postgresStorage interface {
	TableSizeInBytes(table abstract.TableID) (uint64, error)
	EstimateTableRowsCount(table abstract.TableID) (uint64, error)
	ExactTableDescriptionRowsCount(ctx context.Context, table abstract.TableDescription, timeout time.Duration) (uint64, error)
}

type postgresStorageStub struct {
	WholeTableSizeInBytes_    uint64
	WholeTableRowsCount       uint64
	TableDescriptionRowsCount uint64
}

func (m *postgresStorageStub) TableSizeInBytes(_ abstract.TableID) (uint64, error) {
	return m.WholeTableSizeInBytes_, nil
}

func (m *postgresStorageStub) EstimateTableRowsCount(_ abstract.TableID) (uint64, error) {
	return m.WholeTableRowsCount, nil
}

func (m *postgresStorageStub) ExactTableDescriptionRowsCount(_ context.Context, _ abstract.TableDescription, _ time.Duration) (uint64, error) {
	return m.TableDescriptionRowsCount, nil
}

//---

type SplittedTableMetadata struct {
	DataSizeInBytes uint64
	DataSizeInRows  uint64
	PartsCount      uint64
}

type Splitter interface {
	Split(ctx context.Context, table abstract.TableDescription) (*SplittedTableMetadata, error)
}
