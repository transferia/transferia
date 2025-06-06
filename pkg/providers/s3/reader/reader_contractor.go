package reader

import (
	"context"

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	chunk_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
)

type ReaderContractor struct {
	impl Reader
}

func (c *ReaderContractor) Read(ctx context.Context, filePath string, pusher chunk_pusher.Pusher) error {
	err := c.impl.Read(ctx, filePath, pusher)
	if err != nil {
		return xerrors.Errorf("c.impl.Read returned error, err: %w", err)
	}
	chunk := chunk_pusher.Chunk{
		FilePath:  filePath,
		Completed: true,
		Offset:    -1,
		Size:      0,
		Items:     nil,
	}
	err = pusher.Push(ctx, chunk)
	if err != nil {
		return xerrors.Errorf("pusher.Push returned error, err: %w", err)
	}
	return nil
}

func (c *ReaderContractor) ParsePassthrough(chunk chunk_pusher.Chunk) []abstract.ChangeItem {
	return c.impl.ParsePassthrough(chunk)
}

// ObjectsFilter that is default for Reader implementation (e.g. filter that leaves only .parquet files).
func (c *ReaderContractor) ObjectsFilter() ObjectsFilter {
	return c.impl.ObjectsFilter()
}

func (c *ReaderContractor) ResolveSchema(ctx context.Context) (*abstract.TableSchema, error) {
	return c.impl.ResolveSchema(ctx)
}

//---

func (c *ReaderContractor) EstimateRowsCountAllObjects(ctx context.Context) (uint64, error) {
	rowCounter, ok := c.impl.(RowsCountEstimator)
	if !ok {
		return 0, xerrors.Errorf("unable to cast c.impl to RowsCountEstimator, type of c.impl: %T", c.impl)
	}
	return rowCounter.EstimateRowsCountAllObjects(ctx)
}

func (c *ReaderContractor) EstimateRowsCountOneObject(ctx context.Context, obj *aws_s3.Object) (uint64, error) {
	rowCounter, ok := c.impl.(RowsCountEstimator)
	if !ok {
		return 0, xerrors.Errorf("unable to cast c.impl to RowsCountEstimator, type of c.impl: %T", c.impl)
	}
	return rowCounter.EstimateRowsCountOneObject(ctx, obj)
}

func NewReaderContractor(in Reader) *ReaderContractor {
	return &ReaderContractor{
		impl: in,
	}
}
