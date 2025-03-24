package reader

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/providers/s3"
	chunk_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/sync/errgroup"
)

const defaultBlockSize = humanize.MiByte

var (
	_ Reader     = (*GenericParserReader)(nil)
	_ RowCounter = (*GenericParserReader)(nil)
)

type GenericParserReader struct {
	table       abstract.TableID
	bucket      string
	client      s3iface.S3API
	downloader  *s3manager.Downloader
	logger      log.Logger
	tableSchema *abstract.TableSchema
	pathPrefix  string
	blockSize   int64
	pathPattern string
	metrics     *stats.SourceStats
	parser      parsers.Parser
}

func (r *GenericParserReader) openReader(ctx context.Context, filePath string) (*S3Reader, error) {
	sr, err := NewS3Reader(ctx, r.client, r.downloader, r.bucket, filePath, r.metrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to create reader at: %w", err)
	}
	return sr, nil
}

// estimateRows calculates approximate rows count of files.
//
// Implementation:
// 1. Open readers for all files to obtain their sizes.
// 2. Take one random reader, calculate its average line size.
// 3. Divide size of all files by average line size to get result (total rows count).
func (r *GenericParserReader) estimateRows(ctx context.Context, files []*aws_s3.Object) (uint64, error) {
	totalSize := atomic.Int64{}
	var randomReader *S3Reader

	// 1. Open readers for all files to obtain their sizes.
	eg := errgroup.Group{}
	eg.SetLimit(8)
	for _, file := range files {
		eg.Go(func() error {
			var size int64
			if file.Size != nil {
				size = *file.Size
			} else {
				r.logger.Warnf("size of file %s is unknown, will measure", *file.Key)
				reader, err := r.openReader(ctx, *file.Key)
				if err != nil {
					return xerrors.Errorf("unable to open reader for file: %s: %w", *file.Key, err)
				}
				size = reader.Size()
				if randomReader == nil && size > 0 {
					// Since we need just one random reader, race here is not a problem.
					randomReader = reader
				}
			}
			totalSize.Add(size)
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return 0, xerrors.Errorf("unable to open readers: %w", err)
	}

	res := uint64(0)
	if total := totalSize.Load(); total > 0 && randomReader != nil {
		// 2. Take one random reader, calculate its average line size.
		buff, err := r.ParseFile(ctx, "", randomReader)
		if err != nil {
			return 0, xerrors.Errorf("unable to parse: %w", err)
		}
		bytesPerLine := float64(randomReader.Size()) / float64(len(buff))

		// 3. Divide size of all files by average line size to get result (total rows count).
		totalLines := math.Ceil(float64(total) / bytesPerLine)
		res = uint64(totalLines)
	}
	return res, nil
}

func (r *GenericParserReader) RowCount(ctx context.Context, obj *aws_s3.Object) (uint64, error) {
	res, err := r.estimateRows(ctx, []*aws_s3.Object{obj})
	if err != nil {
		return 0, xerrors.Errorf("failed to estimate rows of file: %s : %w", *obj.Key, err)
	}
	return res, nil
}

func (r *GenericParserReader) TotalRowCount(ctx context.Context) (uint64, error) {
	files, err := ListFiles(r.bucket, r.pathPrefix, r.pathPattern, r.client, r.logger, nil, r.ObjectsFilter())
	if err != nil {
		return 0, xerrors.Errorf("unable to load file list: %w", err)
	}

	res, err := r.estimateRows(ctx, files)
	if err != nil {
		return 0, xerrors.Errorf("failed to estimate total rows: %w", err)
	}
	return res, nil
}

func (r *GenericParserReader) Read(ctx context.Context, filePath string, pusher chunk_pusher.Pusher) error {
	s3Reader, err := r.openReader(ctx, filePath)
	if err != nil {
		return xerrors.Errorf("unable to open reader: %w", err)
	}

	readSize := s3Reader.Size()
	buff, err := r.ParseFile(ctx, filePath, s3Reader)
	if err != nil {
		return xerrors.Errorf("unable to parse: %s: %w", filePath, err)
	}

	if len(buff) > 0 {
		if err := pusher.Push(ctx, chunk_pusher.Chunk{
			Items:     buff,
			FilePath:  filePath,
			Offset:    0,
			Completed: true,
			Size:      readSize,
		}); err != nil {
			return xerrors.Errorf("unable to push: %w", err)
		}
	}
	return nil
}

func (r *GenericParserReader) ParsePassthrough(chunk chunk_pusher.Chunk) []abstract.ChangeItem {
	// the most complex and useful method in the world
	return chunk.Items
}

func (r *GenericParserReader) ParseFile(ctx context.Context, filePath string, s3Reader *S3Reader) ([]abstract.ChangeItem, error) {
	offset := 0

	var fullFile []byte
	for {
		select {
		case <-ctx.Done():
			r.logger.Info("Read canceled")
			return nil, nil
		default:
		}
		data := make([]byte, r.blockSize)
		lastRound := false
		n, err := s3Reader.ReadAt(data, int64(offset))
		if err != nil {
			if xerrors.Is(err, io.EOF) && n > 0 {
				data = data[0:n]
				lastRound = true
			} else {
				return nil, xerrors.Errorf("failed to read from file: %w", err)
			}
		}
		offset += n

		fullFile = append(fullFile, data...)
		if lastRound {
			break
		}
	}
	return r.parser.Do(parsers.Message{
		Offset:     0,
		SeqNo:      0,
		Key:        nil,
		CreateTime: s3Reader.LastModified(),
		WriteTime:  s3Reader.LastModified(),
		Value:      fullFile,
		Headers:    nil,
	}, abstract.NewPartition(filePath, 0)), nil
}

func (r *GenericParserReader) ResolveSchema(ctx context.Context) (*abstract.TableSchema, error) {
	if r.tableSchema != nil && len(r.tableSchema.Columns()) != 0 {
		return r.tableSchema, nil
	}

	files, err := ListFiles(r.bucket, r.pathPrefix, r.pathPattern, r.client, r.logger, aws.Int(1), r.ObjectsFilter())
	if err != nil {
		return nil, xerrors.Errorf("unable to load file list: %w", err)
	}

	if len(files) < 1 {
		return nil, xerrors.Errorf("unable to resolve schema, no files found: %s", r.pathPrefix)
	}

	return r.resolveSchema(ctx, *files[0].Key)
}

func (r *GenericParserReader) ObjectsFilter() ObjectsFilter { return IsNotEmpty }

func (r *GenericParserReader) resolveSchema(ctx context.Context, key string) (*abstract.TableSchema, error) {
	s3Reader, err := r.openReader(ctx, key)
	if err != nil {
		return nil, xerrors.Errorf("unable to open reader for file: %s: %w", key, err)
	}

	buff := make([]byte, r.blockSize)
	read, err := s3Reader.ReadAt(buff, 0)
	if err != nil && !xerrors.Is(err, io.EOF) {
		return nil, xerrors.Errorf("failed to read sample from file: %s: %w", key, err)
	}
	if read == 0 {
		// read nothing, file was empty
		return nil, xerrors.New(fmt.Sprintf("could not read sample data from file: %s", key))
	}

	reader := bufio.NewReader(bytes.NewReader(buff))
	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, xerrors.Errorf("failed to read sample content for schema deduction: %w", err)
	}
	changes := r.parser.Do(parsers.Message{
		Offset:     0,
		SeqNo:      0,
		Key:        []byte(key),
		CreateTime: s3Reader.LastModified(),
		WriteTime:  s3Reader.LastModified(),
		Value:      content,
		Headers:    nil,
	}, abstract.NewPartition(key, 0))

	if len(changes) == 0 {
		return nil, xerrors.Errorf("unable to parse sample data: %v", util.Sample(string(content), 1024))
	}

	r.tableSchema = changes[0].TableSchema
	return r.tableSchema, nil
}

func NewGenericParserReader(src *s3.S3Source, lgr log.Logger, sess *session.Session, metrics *stats.SourceStats, parser parsers.Parser) (*GenericParserReader, error) {
	reader := &GenericParserReader{
		table: abstract.TableID{
			Namespace: src.TableNamespace,
			Name:      src.TableName,
		},
		bucket:      src.Bucket,
		client:      aws_s3.New(sess),
		downloader:  s3manager.NewDownloader(sess),
		logger:      lgr,
		tableSchema: abstract.NewTableSchema(src.OutputSchema),
		pathPrefix:  src.PathPrefix,
		blockSize:   defaultBlockSize,
		pathPattern: src.PathPattern,
		metrics:     metrics,
		parser:      parser,
	}

	if len(reader.tableSchema.Columns()) == 0 {
		var err error
		reader.tableSchema, err = reader.ResolveSchema(context.Background())
		if err != nil {
			return nil, xerrors.Errorf("unable to resolve schema: %w", err)
		}
	}

	return reader, nil
}
