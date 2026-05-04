package reader

import (
	"context"
	"sync"

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/pkg/abstract"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
)

// ReaderContractor - guarantees contract:
//   - Read() should end with 'Chunk' with offset=-1
//
// that allows us to implement each reader (format), not keep in thoughts this contract
//
// Read uses a schema resolved and cached by ResolveSchema (first successful resolution is reused).
//
// schemaBackoff (when non-nil) retries ExecuteSchemaResolver during first schema cache fill — safe, no sink side effects.
// readBackoff (when non-nil) retries the format Read and completion Push — repeating Read after partial progress
// can duplicate rows at the sink; factory enables schema backoff only (see factory.New).
//
// Errors: concrete reader_error.ReaderError kinds are preserved through WrapContractorReadStep; outer
// code may use reader_error.ClassifyContractorError.
type ReaderContractor struct {
	s3Reader         S3Reader
	s3SchemaResolver S3SchemaResolver

	schemaBackoff  backoff.BackOff // retries for ExecuteSchemaResolver until s3SchemaResolver is cached
	readBackoff    backoff.BackOff // retries for S3Reader.Read + completion Push (optional; duplicates risk)
	unparsedPolicy s3_model.UnparsedPolicy

	schemaMu     sync.Mutex
	schemaCached *abstract.TableSchema
}

var _ Reader = (*ReaderContractor)(nil)

// ContractorOption configures ReaderContractor construction.
type ContractorOption func(*ReaderContractor)

// Read implements Reader. When readBackoff is set, transport/sink failures may retry (full Read repeats).
// Schema resolution errors (including ReaderErrorNoFiles) are wrapped with contractor.Read.ResolveSchema
// so callers can use reader_error.AsReaderErrorNoFiles after unwrapping.
func (c *ReaderContractor) Read(ctx context.Context, filePath string, pusher s3_pusher.Pusher) reader_error.ReaderError {
	sch, err := c.resolveSchemaCached(ctx)
	if err != nil {
		return reader_error.WrapContractorReadStep("contractor.Read.ResolveSchema", err)
	}

	readOnce := func() reader_error.ReaderError {
		e := c.s3Reader.Read(ctx, sch, filePath, pusher)
		if e != nil {
			return reader_error.WrapContractorReadStep("contractor.Read.impl.Read", e)
		}
		chunk := s3_pusher.Chunk{
			FilePath:  filePath,
			Completed: true,
			Offset:    -1,
			Size:      0,
			Items:     nil,
		}
		err2 := pusher.Push(ctx, chunk)
		if err2 != nil {
			sinkErr := reader_error.ReaderErrorFromPush("pusher.Push.completed", filePath, err2)
			return reader_error.WrapContractorReadStep("contractor.Read.completedChunk", sinkErr)
		}
		return nil
	}

	if c.readBackoff == nil {
		return readOnce()
	}
	return reader_error.RunWithContractorRetry(ctx, c.unparsedPolicy, reader_error.ContractorPhaseRead, c.readBackoff, readOnce)
}

func (c *ReaderContractor) resolveSchemaCached(ctx context.Context) (*abstract.TableSchema, reader_error.ReaderError) {
	c.schemaMu.Lock()
	if c.schemaCached != nil {
		s := c.schemaCached
		c.schemaMu.Unlock()
		return s, nil
	}
	c.schemaMu.Unlock()

	if c.schemaBackoff == nil {
		c.schemaMu.Lock()
		defer c.schemaMu.Unlock()

		if c.schemaCached != nil {
			return c.schemaCached, nil
		}
		sch, err := ExecuteSchemaResolver(ctx, c.s3SchemaResolver)
		if err != nil {
			return nil, err
		}
		c.schemaCached = sch
		return c.schemaCached, nil
	}

	var resolved *abstract.TableSchema
	re := reader_error.RunWithContractorRetry(
		ctx,
		c.unparsedPolicy,
		reader_error.ContractorPhaseResolveSchema,
		c.schemaBackoff,
		func() reader_error.ReaderError {
			c.schemaMu.Lock()
			if c.schemaCached != nil {
				resolved = c.schemaCached
				c.schemaMu.Unlock()
				return nil
			}
			c.schemaMu.Unlock()

			sch, err := ExecuteSchemaResolver(ctx, c.s3SchemaResolver)
			if err != nil {
				return err
			}
			c.schemaMu.Lock()
			defer c.schemaMu.Unlock()

			if c.schemaCached == nil {
				c.schemaCached = sch
			}
			resolved = c.schemaCached
			return nil
		},
	)
	if re != nil {
		return nil, re
	}
	return resolved, nil
}

// ResolveSchema returns the cached schema after the first successful inference walk.
// On ReaderErrorNoFiles (empty prefix / pattern), callers may use reader_error.AsReaderErrorNoFiles.
func (c *ReaderContractor) ResolveSchema(ctx context.Context) (*abstract.TableSchema, reader_error.ReaderError) {
	sch, err := c.resolveSchemaCached(ctx)
	if err != nil {
		return nil, reader_error.WrapContractorReadStep("contractor.ResolveSchema", err)
	}
	return sch, nil
}

//---

// EstimateRowsCountAllObjects forwards to the reader
func (c *ReaderContractor) EstimateRowsCountAllObjects(ctx context.Context) (uint64, error) {
	return c.s3Reader.EstimateRowsCountAllObjects(ctx)
}

// EstimateRowsCountOneObject forwards to the reader
func (c *ReaderContractor) EstimateRowsCountOneObject(ctx context.Context, obj *aws_s3.Object) (uint64, error) {
	return c.s3Reader.EstimateRowsCountOneObject(ctx, obj)
}

func NewReaderContractor(read S3Reader, schema S3SchemaResolver, opts ...ContractorOption) *ReaderContractor {
	c := &ReaderContractor{
		s3Reader:         read,
		s3SchemaResolver: schema,

		schemaBackoff:  nil,
		readBackoff:    nil,
		unparsedPolicy: "", schemaMu: sync.Mutex{},
		schemaCached: nil,
	}
	if l := schema.SchemaWalkListing(); l != nil {
		c.unparsedPolicy = l.Policy
	}
	for _, o := range opts {
		o(c)
	}
	return c
}
