package table_part_provider

import (
	"context"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
)

type AbstractTablePartProviderGetter interface {
	NextOperationTablePart(context.Context) (*abstract.OperationTablePart, error)
}

type AbstractTablePartProviderSetter interface {
	AppendParts(ctx context.Context, parts []*abstract.OperationTablePart) error
	EnrichShardedState(state string) (string, error)
	AllPartsOrNil() []*abstract.OperationTablePart // for sync - all parts, for async - nil
	AsyncLoadPartsIfNeeded(
		ctx context.Context,
		storage abstract.Storage,
		tables []abstract.TableDescription,
		cp coordinator.Coordinator,
		transferID string,
		operationID string,
		checkLoaderError func() error,
	) error
}

type AbstractTablePartProviderFull interface {
	AbstractTablePartProviderGetter
	AbstractTablePartProviderSetter
}
