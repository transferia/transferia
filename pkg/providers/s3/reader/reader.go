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
	IsObj(file *aws_s3.Object) bool
	ResolveSchema(ctx context.Context) (*abstract.TableSchema, error)
}

type RowCounter interface {
	TotalRowCount(ctx context.Context) (uint64, error)
	RowCount(ctx context.Context, obj *aws_s3.Object) (uint64, error)
}

// SkipObject returns true if an object should be skipped.
// An object is skipped if the file type does not match the one covered by the reader or
// if the objects name/path is not included in the path pattern.
func SkipObject(file *aws_s3.Object, pathPattern, splitter string, isObj IsObj) bool {
	if file == nil {
		return true
	}

	return !(isObj(file) && glob.SplitMatch(pathPattern, *file.Key, splitter))
}

type IsObj func(file *aws_s3.Object) bool

// ListFiles lists all files matching the pathPattern in a bucket.
// A fast circuit breaker is built in for schema resolution where we do not need the full list of objects.
func ListFiles(bucket, pathPrefix, pathPattern string, client s3iface.S3API, logger log.Logger, maxResults *int, isObj IsObj) ([]*aws_s3.Object, error) {
	var currentMarker *string
	var res []*aws_s3.Object
	fastStop := false
	for {
		files, err := client.ListObjects(&aws_s3.ListObjectsInput{
			Bucket:  aws.String(bucket),
			Prefix:  aws.String(pathPrefix),
			MaxKeys: aws.Int64(1000), // set explicitly
			Marker:  currentMarker,
		})
		if err != nil {
			return nil, xerrors.Errorf("unable to load file list: %w", err)
		}

		for _, file := range files.Contents {
			if SkipObject(file, pathPattern, "|", isObj) {
				logger.Infof("file did not pass type/path check, skipping: file %s, pathPattern: %s", *file.Key, pathPattern)
				continue
			}
			res = append(res, file)

			// for schema resolution we can stop the process of file fetching faster since we need only 1 file
			if maxResults != nil && *maxResults == len(res) {
				fastStop = true
				break
			}
		}
		if len(files.Contents) > 0 {
			currentMarker = files.Contents[len(files.Contents)-1].Key
		}

		if fastStop || len(files.Contents) < 1000 {
			break
		}
	}

	return res, nil
}

func appendSystemColsTableSchema(cols []abstract.ColSchema) *abstract.TableSchema {
	fileName := abstract.NewColSchema(FileNameSystemCol, schema.TypeString, true)
	rowIndex := abstract.NewColSchema(RowIndexSystemCol, schema.TypeUint64, true)
	cols = append([]abstract.ColSchema{fileName, rowIndex}, cols...)
	return abstract.NewTableSchema(cols)
}

func New(
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
			return nil, xerrors.Errorf("failed to initialize new csv reader: %w", err)
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
