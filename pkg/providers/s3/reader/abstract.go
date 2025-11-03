package reader

import (
	"context"

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/s3/pusher"
	"go.ytsaurus.tech/yt/go/schema"
)

var (
	FileNameSystemCol = "__file_name"
	RowIndexSystemCol = "__row_index"

	EstimateFilesLimit = 10

	SystemColumnNames = map[string]bool{FileNameSystemCol: true, RowIndexSystemCol: true}
)

type Reader interface {
	Read(ctx context.Context, filePath string, pusher pusher.Pusher) error

	// ParsePassthrough is used in the parsqueue pusher for replications.
	// Since actual parsing in the S3 parsers is a rather complex process, tailored to each format, this methods
	// is just mean as a simple passthrough to fulfill the parsqueue signature contract and forwards the already parsed CI elements for pushing.
	ParsePassthrough(chunk pusher.Chunk) []abstract.ChangeItem

	// ObjectsFilter that is default for Reader implementation (e.g. filter that leaves only .parquet files).
	ObjectsFilter() ObjectsFilter

	ResolveSchema(ctx context.Context) (*abstract.TableSchema, error)
}

type RowsCountEstimator interface {
	EstimateRowsCountAllObjects(ctx context.Context) (uint64, error)
	EstimateRowsCountOneObject(ctx context.Context, obj *aws_s3.Object) (uint64, error)
}

// ObjectsFilter returns true for needful objects, false for objects that should be ignored (skipped).
type ObjectsFilter func(file *aws_s3.Object) bool

var _ ObjectsFilter = IsNotEmpty

// IsNotEmpty can be used as common filter that skips empty files.
func IsNotEmpty(file *aws_s3.Object) bool {
	if file.Size == nil || *file.Size == 0 {
		return false
	}
	return true
}

func AppendSystemColsTableSchema(cols []abstract.ColSchema, isPkey bool) *abstract.TableSchema {
	fileName := abstract.NewColSchema(FileNameSystemCol, schema.TypeString, isPkey)
	rowIndex := abstract.NewColSchema(RowIndexSystemCol, schema.TypeUint64, isPkey)
	cols = append([]abstract.ColSchema{fileName, rowIndex}, cols...)
	return abstract.NewTableSchema(cols)
}

func FlushChunk(
	ctx context.Context,
	filePath string,
	offset uint64,
	currentSize int64,
	buff []abstract.ChangeItem,
	somePusher pusher.Pusher,
) error {
	if len(buff) == 0 {
		return nil
	}

	chunk := pusher.NewChunk(filePath, false, offset, currentSize, buff)
	if err := somePusher.Push(ctx, chunk); err != nil {
		return err
	}

	return nil
}
