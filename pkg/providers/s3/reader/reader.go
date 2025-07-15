package reader

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers/registry/protobuf/protoparser"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/pusher"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util/glob"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
)

var (
	FileNameSystemCol = "__file_name"
	RowIndexSystemCol = "__row_index"

	EstimateFilesLimit = 10

	systemColumnNames = map[string]bool{FileNameSystemCol: true, RowIndexSystemCol: true}
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

// SkipObject returns true if an object should be skipped.
// An object is skipped if the file type does not match the one covered by the reader or
// if the objects name/path is not included in the path pattern or if custom filter returned false.
func SkipObject(file *aws_s3.Object, pathPattern, splitter string, filter ObjectsFilter) bool {
	if file == nil {
		return true
	}
	keepObject := filter(file) && glob.SplitMatch(pathPattern, *file.Key, splitter)
	return !keepObject
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

// ListFiles lists all files matching the pathPattern in a bucket.
// A fast circuit breaker is built in for schema resolution where we do not need the full list of objects.
func ListFiles(bucket, pathPrefix, pathPattern string, client s3iface.S3API, logger log.Logger, limit *int, filter ObjectsFilter) ([]*aws_s3.Object, error) {
	var currentMarker *string
	var res []*aws_s3.Object
	fastStop := false
	for {
		listBatchSize := int64(1000)
		if limit != nil {
			remaining := max(0, int64(*limit-len(res)))
			// For example, if remaining == 1, its more effective to list 1 object than 1000.
			listBatchSize = min(listBatchSize, remaining)
		}
		files, err := client.ListObjects(&aws_s3.ListObjectsInput{
			Bucket:  aws.String(bucket),
			Prefix:  aws.String(pathPrefix),
			MaxKeys: aws.Int64(listBatchSize),
			Marker:  currentMarker,
		})
		if err != nil {
			return nil, xerrors.Errorf("unable to load file list: %w", err)
		}

		for _, file := range files.Contents {
			if SkipObject(file, pathPattern, "|", filter) {
				logger.Debugf("ListFiles - file did not pass type/path check, skipping: file %s, pathPattern: %s", *file.Key, pathPattern)
				continue
			}
			res = append(res, file)

			// for schema resolution we can stop the process of file fetching faster since we need only 1 file
			if limit != nil && *limit == len(res) {
				fastStop = true
				break
			}
		}
		if len(files.Contents) > 0 {
			currentMarker = files.Contents[len(files.Contents)-1].Key
		}

		if fastStop || int64(len(files.Contents)) < listBatchSize {
			break
		}
	}

	return res, nil
}

// FileSize returns file's size if it stored in file.Size, otherwise it gets size by S3 API call.
// NOTE: FileSize only returns file's size and do NOT changes original file.Size field.
func FileSize(bucket string, file *aws_s3.Object, client s3iface.S3API, logger log.Logger) (uint64, error) {
	if file == nil {
		return 0, xerrors.New("provided file is nil")
	}
	if file.Key == nil {
		return 0, xerrors.New("provided file key is nil")
	}
	if file.Size != nil {
		if *file.Size < 0 {
			return 0, xerrors.Errorf("size of file %s is negative (%d)", *file.Key, *file.Size)
		}
		return uint64(*file.Size), nil
	}
	logger.Debugf("Size of file %s is unknown, measuring it", *file.Key)
	resp, err := client.GetObjectAttributes(&aws_s3.GetObjectAttributesInput{
		Bucket:           aws.String(bucket),
		Key:              aws.String(*file.Key),
		ObjectAttributes: aws.StringSlice([]string{aws_s3.ObjectAttributesObjectSize}),
	})
	if err != nil {
		return 0, xerrors.Errorf("unable to get file %s size attribute: %w", *file.Key, err)
	}
	if resp.ObjectSize == nil {
		return 0, xerrors.Errorf("returned by s3-api size of file %s is nil", *file.Key)
	}
	if *resp.ObjectSize < 0 {
		return 0, xerrors.Errorf("measured size of file %s is negative (%d)", *file.Key, *resp.ObjectSize)
	}
	return uint64(*resp.ObjectSize), nil
}

func appendSystemColsTableSchema(cols []abstract.ColSchema) *abstract.TableSchema {
	fileName := abstract.NewColSchema(FileNameSystemCol, schema.TypeString, true)
	rowIndex := abstract.NewColSchema(RowIndexSystemCol, schema.TypeUint64, true)
	cols = append([]abstract.ColSchema{fileName, rowIndex}, cols...)
	return abstract.NewTableSchema(cols)
}

func newImpl(
	src *s3.S3Source,
	lgr log.Logger,
	sess *session.Session,
	metrics *stats.SourceStats,
) (Reader, error) {
	switch src.InputFormat {
	case model.ParsingFormatPARQUET:
		reader, err := NewParquet(src, lgr, sess, metrics)
		if err != nil {
			return nil, xerrors.Errorf("failed to initialize new parquet reader: %w", err)
		}
		return reader, nil
	case model.ParsingFormatJSON:
		reader, err := NewJSONParserReader(src, lgr, sess, metrics)
		if err != nil {
			return nil, xerrors.Errorf("failed to initialize new json reader: %w", err)
		}
		return reader, nil
	case model.ParsingFormatJSONLine:
		reader, err := NewJSONLineReader(src, lgr, sess, metrics)
		if err != nil {
			return nil, xerrors.Errorf("failed to initialize new jsonline reader: %w", err)
		}
		return reader, nil
	case model.ParsingFormatCSV:
		reader, err := NewCSVReader(src, lgr, sess, metrics)
		if err != nil {
			return nil, xerrors.Errorf("failed to initialize new csv reader: %w", err)
		}
		return reader, nil
	case model.ParsingFormatPROTO:
		if len(src.Format.ProtoParser.DescFile) == 0 {
			return nil, xerrors.New("desc file required")
		}
		// this is magic field to get descriptor from YT
		if len(src.Format.ProtoParser.DescResourceName) != 0 {
			return nil, xerrors.New("desc resource name is not supported by S3 source")
		}
		cfg := new(protoparser.ProtoParserConfig)
		cfg.IncludeColumns = src.Format.ProtoParser.IncludeColumns
		cfg.PrimaryKeys = src.Format.ProtoParser.PrimaryKeys
		cfg.NullKeysAllowed = src.Format.ProtoParser.NullKeysAllowed
		if err := cfg.SetDescriptors(
			src.Format.ProtoParser.DescFile,
			src.Format.ProtoParser.MessageName,
			src.Format.ProtoParser.PackageType,
		); err != nil {
			return nil, xerrors.Errorf("SetDescriptors error: %v", err)
		}
		cfg.SetLineSplitter(src.Format.ProtoParser.PackageType)
		cfg.SetScannerType(src.Format.ProtoParser.PackageType)

		parser, err := protoparser.NewProtoParser(cfg, metrics)
		if err != nil {
			return nil, xerrors.Errorf("unable to construct proto parser: %w", err)
		}
		reader, err := NewGenericParserReader(
			src,
			lgr,
			sess,
			metrics,
			parser,
		)
		if err != nil {
			return nil, xerrors.Errorf("failed to initialize new generic reader: %w", err)
		}
		return reader, nil
	case model.ParsingFormatLine:
		reader, err := NewLineReader(src, lgr, sess, metrics)
		if err != nil {
			return nil, xerrors.Errorf("failed to initialize new line reader: %w", err)
		}
		return reader, nil
	default:
		return nil, xerrors.Errorf("unknown format: %s", src.InputFormat)
	}
}

func New(
	src *s3.S3Source,
	lgr log.Logger,
	sess *session.Session,
	metrics *stats.SourceStats,
) (Reader, error) {
	result, err := newImpl(src, lgr, sess, metrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to create new reader: %w", err)
	}
	return NewReaderContractor(result), nil
}
