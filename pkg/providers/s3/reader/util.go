package reader

import (
	"context"
	"io"
	"time"

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/providers/s3/s3util"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

// ExecuteSchemaResolver runs the common preset-or-S3-walk resolution for a format S3SchemaResolver.
func ExecuteSchemaResolver(ctx context.Context, r S3SchemaResolver) (*abstract.TableSchema, reader_error.ReaderError) {
	cfg := r.SchemaWalkListing()
	if cfg == nil {
		return nil, reader_error.NewReaderErrorConfig(
			"schema.resolve",
			xerrors.New("schema resolver returned nil listing config"),
		)
	}
	if cfg.TableSchema != nil && len(cfg.TableSchema.Columns()) != 0 {
		return cfg.TableSchema, nil
	}
	if cfg.Client == nil || cfg.Logger == nil {
		if cfg.TableSchema != nil {
			return cfg.TableSchema, nil
		}
		return nil, reader_error.NewReaderErrorConfig(
			"schema.resolve",
			xerrors.New("schema listing missing S3 client/logger and no usable preset"),
		)
	}
	sch, err := walkS3ObjectsForSchemaInference(
		ctx,
		cfg.Policy,
		cfg.Bucket,
		cfg.PathPrefix,
		cfg.PathPattern,
		cfg.Client,
		cfg.Logger,
		cfg.ObjectsFilter,
		cfg.ListOpLabel,
		r.TryInferSchemaFromObject,
	)
	if err != nil {
		if cfg.WrapInferErrorOp != "" {
			return nil, reader_error.WrapReaderError(cfg.WrapInferErrorOp, err)
		}
		return nil, err
	}
	return sch, nil
}

// walkS3ObjectsForSchemaInference lists objects (streaming via ListFilesWithCallback) and calls try until
// a non-empty schema is returned. When policy is UnparsedPolicyContinue, recoverable sample errors
// (ReaderErrorData) cause the next object to be tried; transport/sink/fatal/config/no-files stop immediately.
func walkS3ObjectsForSchemaInference(
	ctx context.Context,
	policy s3_model.UnparsedPolicy,
	bucket string,
	pathPrefix string,
	pathPattern string,
	client s3iface.S3API,
	logger log.Logger,
	filter ObjectsFilter,
	listOp string,
	try func(ctx context.Context, key string) (*abstract.TableSchema, reader_error.ReaderError),
) (*abstract.TableSchema, reader_error.ReaderError) {
	var lastErr reader_error.ReaderError
	var sawObject bool
	var out *abstract.TableSchema

	err := s3util.ListFilesWithCallback(
		bucket,
		pathPrefix,
		pathPattern,
		client,
		logger,
		filter,
		func(obj *aws_s3.Object) (bool, error) {
			sawObject = true
			sch, rerr := try(ctx, *obj.Key)
			if rerr == nil {
				if sch != nil && len(sch.Columns()) != 0 {
					out = sch
					return true, nil
				}
				return false, nil
			}
			lastErr = rerr
			if reader_error.SchemaSampleErrorMayFallThroughToNextFile(policy, rerr) {
				return false, nil
			}
			return true, rerr
		},
	)
	if err != nil {
		return nil, reader_error.NewReaderErrorTransport(listOp, pathPrefix, err)
	}
	if out != nil {
		return out, nil
	}
	if !sawObject {
		return nil, reader_error.NewReaderErrorNoFiles("schemaWalk.ResolveSchema", pathPrefix)
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, reader_error.NewReaderErrorDataSchema("schemaWalk.empty_inference", pathPrefix, xerrors.New("no schema inferred from listed objects"))
}

// ReadSchemaInferenceFirstChunk reads the first chunk from an S3 object for schema inference.
func ReadSchemaInferenceFirstChunk(
	ctx context.Context,
	objectKey string,
	transportOpenFile string,
	blockSize int,
	logger log.Logger,
	open func() (s3raw.S3RawReader, error),
	transportOpenOp string,
	transportChunkOp string,
	emptySampleOp string,
) ([]byte, reader_error.ReaderError) {
	_ = ctx
	_ = emptySampleOp
	raw, err := open()
	if err != nil {
		return nil, reader_error.NewReaderErrorTransport(transportOpenOp, transportOpenFile, err)
	}
	cr := NewChunkReader(raw, blockSize, logger)
	defer cr.Close()

	err = cr.ReadNextChunk()
	if err != nil && !xerrors.Is(err, io.EOF) {
		return nil, reader_error.NewReaderErrorTransport(transportChunkOp, objectKey, err)
	}
	buff := cr.Data()
	if len(buff) == 0 {
		return make([]byte, 0), nil
	}
	return buff, nil
}

// TryHandleDataErrorWithFlush handles err when it unwraps to ReaderErrorData: applies UnparsedPolicy via
// HandleDataError and flushes any resulting unparsed marker rows. flushWrap re-wraps FlushChunk failures
// with a format-specific operation path.
//
// If err is not a data-layer reader error, returns handled=false and result=nil (caller should wrap err).
// If handling or flush fails, returns handled=true and a non-nil result.
// If handling succeeded, returns handled=true and result=nil (caller may still apply policy-specific logic).
func TryHandleDataErrorWithFlush(
	ctx context.Context,
	table abstract.TableID,
	policy s3_model.UnparsedPolicy,
	filePath string,
	flushOffset uint64,
	flushParsedSize int64,
	err error,
	pusher s3_pusher.Pusher,
	flushWrap func(reader_error.ReaderError) reader_error.ReaderError,
) (handled bool, result reader_error.ReaderError) {
	errorData, ok := reader_error.AsReaderErrorData(err)
	if !ok {
		return false, nil
	}
	items, handleErr := reader_error.HandleDataError(table, policy, errorData)
	if handleErr != nil {
		return true, handleErr
	}
	if len(items) != 0 {
		if flushErr := FlushChunk(ctx, filePath, flushOffset, flushParsedSize, items, pusher); flushErr != nil {
			return true, flushWrap(flushErr)
		}
	}
	return true, nil
}

// ObjectsFilter returns true for needful objects, false for objects that should be ignored (skipped).
type ObjectsFilter func(file *aws_s3.Object) bool

var (
	_ ObjectsFilter = IsNotEmpty
	_ ObjectsFilter = AcceptAllObjects
)

// IsNotEmpty can be used as common filter that skips empty files.
func IsNotEmpty(file *aws_s3.Object) bool {
	if file.Size == nil || *file.Size == 0 {
		return false
	}
	return true
}

// AcceptAllObjects lists every matched object including zero-byte keys (used for schema inference walks).
func AcceptAllObjects(*aws_s3.Object) bool {
	return true
}

func AppendSystemColsTableSchema(cols []abstract.ColSchema, isPkey bool) *abstract.TableSchema {
	fileName := abstract.NewColSchema(FileNameSystemCol, ytschema.TypeString, isPkey)
	rowIndex := abstract.NewColSchema(RowIndexSystemCol, ytschema.TypeUint64, isPkey)
	cols = append([]abstract.ColSchema{fileName, rowIndex}, cols...)
	return abstract.NewTableSchema(cols)
}

func FlushChunk(
	ctx context.Context,
	filePath string,
	offset uint64,
	currentSize int64,
	changeItems []abstract.ChangeItem,
	somePusher s3_pusher.Pusher,
) reader_error.ReaderError {
	// Format Read paths typically wrap failures with reader_error.WrapReaderError("<format>.Read.FlushChunk", err).
	if len(changeItems) == 0 {
		return nil
	}

	chunk := s3_pusher.NewChunk(filePath, false, offset, currentSize, changeItems)
	if err := somePusher.Push(ctx, chunk); err != nil {
		return reader_error.ReaderErrorFromPush("pusher.Push", filePath, err)
	}

	return nil
}

func withSchemaBackoff(b backoff.BackOff) ContractorOption {
	return func(c *ReaderContractor) {
		c.schemaBackoff = b
	}
}

// withContractorBackoff sets the same backoff for schema resolution and Read (tests / advanced callers).
func withContractorBackoff(b backoff.BackOff) ContractorOption {
	return func(c *ReaderContractor) {
		c.schemaBackoff = b
		c.readBackoff = b
	}
}

func DefaultContractorBackoff() backoff.BackOff {
	b := util.NewExponentialBackOff()
	b.InitialInterval = 500 * time.Millisecond
	b.MaxInterval = 2 * time.Minute
	b.Reset()
	return b
}

// ParsePassthroughChunk is the parsequeue parse function for S3 replication: items are already parsed.
func ParsePassthroughChunk(chunk s3_pusher.Chunk) ([]abstract.ChangeItem, error) {
	return chunk.Items, nil
}

func DataTypes(columns abstract.TableColumns) []string {
	result := make([]string, len(columns))
	for i, column := range columns {
		result[i] = column.DataType
	}
	return result
}
