package reader

import (
	"context"

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/pkg/abstract"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	"go.ytsaurus.tech/library/go/core/log"
)

var (
	FileNameSystemCol = "__file_name"
	RowIndexSystemCol = "__row_index"

	EstimateFilesLimit = 10

	SystemColumnNames = map[string]bool{FileNameSystemCol: true, RowIndexSystemCol: true}
)

//-----------------------------------------------------------------------------
// external interfaces

// Reader is the full reader contract used outside this package (implemented by ReaderContractor).
// ObjectsFilter on ReaderContractor remains IsNotEmpty; schema walk listing uses SchemaWalkListing.ObjectsFilter (AcceptAllObjects).
type Reader interface {
	EstimateRowsCountAllObjects(ctx context.Context) (uint64, error)
	EstimateRowsCountOneObject(ctx context.Context, obj *aws_s3.Object) (uint64, error)

	ResolveSchema(ctx context.Context) (*abstract.TableSchema, reader_error.ReaderError)
	Read(ctx context.Context, filePath string, pusher s3_pusher.Pusher) reader_error.ReaderError
}

//-----------------------------------------------------------------------------
// internal interfaces

// SchemaWalkListing carries TableSchema and S3 listing parameters for sample-based schema inference.
// When TableSchema is non-nil and already has columns, ExecuteSchemaResolver returns it without listing S3.
type SchemaWalkListing struct {
	TableSchema   *abstract.TableSchema
	Policy        s3_model.UnparsedPolicy
	Bucket        string
	PathPrefix    string
	PathPattern   string
	Client        s3iface.S3API
	Logger        log.Logger
	ObjectsFilter ObjectsFilter
	// ListOpLabel is used as the operation tag on transport errors from listing (e.g. "csv.ListFiles").
	ListOpLabel string
	// WrapInferErrorOp, if non-empty, wraps non-nil errors from the walk/inference step with reader_error.WrapReaderError.
	WrapInferErrorOp string
}

// S3SchemaResolver is format-specific schema resolution for ReaderContractor.
// SchemaWalkListing describes preset + listing; TryInferSchemaFromObject samples one object key during a walk.
type S3SchemaResolver interface {
	SchemaWalkListing() *SchemaWalkListing
	TryInferSchemaFromObject(ctx context.Context, objectKey string) (*abstract.TableSchema, reader_error.ReaderError)
}

// S3Reader performs format-specific reading when the table schema is already resolved.
// Registry implementations return this together with S3SchemaResolver from RegisterReader constructors;
// ReaderContractor resolves the schema once, caches it, and passes it to S3Reader.Read on every Read.
type S3Reader interface {
	EstimateRowsCountAllObjects(ctx context.Context) (uint64, error)
	EstimateRowsCountOneObject(ctx context.Context, obj *aws_s3.Object) (uint64, error)

	Read(ctx context.Context, schema *abstract.TableSchema, filePath string, pusher s3_pusher.Pusher) reader_error.ReaderError
}
