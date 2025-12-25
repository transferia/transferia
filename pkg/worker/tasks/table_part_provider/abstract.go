package table_part_provider

import (
	"context"

	"github.com/transferia/transferia/pkg/abstract"
)

type AbstractTablePartProviderGetter interface {
	SharedMemory() abstract.SharedMemory
	NextOperationTablePart(context.Context) (*abstract.OperationTablePart, error)
}

type AbstractTablePartProviderSetter interface {
	AllPartsOrNil() []*abstract.OperationTablePart
	EnrichShardedState(state string) (string, error)
	AsyncLoadPartsIfNeeded(
		ctx context.Context,
		storage abstract.Storage,
		tables []abstract.TableDescription,
		transferID string,
		operationID string,
		checkLoaderError func() error,
	) error
}
