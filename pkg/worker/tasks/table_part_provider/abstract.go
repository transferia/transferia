package table_part_provider

import (
	"context"

	"github.com/transferia/transferia/pkg/abstract"
)

type AbstractTablePartProviderGetter interface {
	SharedMemory() abstract.SharedMemory

	NextOperationTablePart(context.Context) (*abstract.OperationTablePart, error)

	ConvertToTableDescription(in *abstract.OperationTablePart) (*abstract.TableDescription, error) // just pass to shared_memory
	RemoveFromAsyncSharedMemory(in *abstract.OperationTablePart) error
}

type AbstractTablePartProviderSetter interface {
	SharedMemory() abstract.SharedMemory

	AppendParts(ctx context.Context, parts []*abstract.OperationTablePart) error
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
