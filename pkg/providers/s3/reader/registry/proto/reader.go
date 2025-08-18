package proto

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/registry/protobuf/protoparser"
	"github.com/transferia/transferia/pkg/providers/s3"
	chunk_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	abstract_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/providers/s3/s3util"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/sync/errgroup"
)

func init() {
	abstract_reader.RegisterReader(model.ParsingFormatPROTO, NewProtoReader)
}

const (
	perPushBatchSize = 15 * humanize.MiByte
	defaultBlockSize = humanize.MiByte
)

var (
	_ abstract_reader.Reader             = (*ProtoReader)(nil)
	_ abstract_reader.RowsCountEstimator = (*ProtoReader)(nil)
)

type ProtoReader struct {
	table          abstract.TableID
	bucket         string
	client         s3iface.S3API
	downloader     *s3manager.Downloader
	logger         log.Logger
	tableSchema    *abstract.TableSchema
	pathPrefix     string
	blockSize      int64
	pathPattern    string
	metrics        *stats.SourceStats
	parserBuilder  parsers.ParserBuilder
	unparsedPolicy s3.UnparsedPolicy
}

func NewProtoReader(src *s3.S3Source, lgr log.Logger, sess *session.Session, metrics *stats.SourceStats) (abstract_reader.Reader, error) {
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

	parserBuilder, err := protoparser.NewLazyProtoParserBuilder(cfg, metrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to construct proto parser: %w", err)
	}
	return newReaderImpl(src, lgr, sess, metrics, parserBuilder)
}

func (r *ProtoReader) newS3RawReader(ctx context.Context, filePath string) (s3raw.S3RawReader, error) {
	sr, err := s3raw.NewS3RawReader(ctx, r.client, r.bucket, filePath, r.metrics)
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
func (r *ProtoReader) estimateRows(ctx context.Context, files []*aws_s3.Object) (uint64, error) {
	totalSize := atomic.Int64{}
	var randomReader s3raw.S3RawReader
	var randomKey *string

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
				s3RawReader, err := r.newS3RawReader(ctx, *file.Key)
				if err != nil {
					return xerrors.Errorf("unable to open s3RawReader for file: %s: %w", *file.Key, err)
				}
				size = s3RawReader.Size()
				if randomReader == nil && size > 0 {
					// Since we need just one random s3RawReader, race here is not a problem.
					randomReader = s3RawReader
					randomKey = file.Key
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
		linesCount, err := r.countLines(ctx, randomReader, randomKey)
		if err != nil {
			return 0, xerrors.Errorf("unable to parse: %w", err)
		}
		bytesPerLine := float64(randomReader.Size()) / float64(linesCount)

		// 3. Divide size of all files by average line size to get result (total rows count).
		totalLines := math.Ceil(float64(total) / bytesPerLine)
		res = uint64(totalLines)
	}
	return res, nil
}

func (r *ProtoReader) countLines(ctx context.Context, reader s3raw.S3RawReader, key *string) (int, error) {
	res := 0

	counter := func(items []abstract.ChangeItem) error {
		res += len(items)
		return nil
	}

	var keyStr string
	if key != nil {
		keyStr = *key
	}

	if err := r.Read(context.Background(), keyStr, chunk_pusher.NewSyncPusher(counter)); err != nil {
		return 0, xerrors.Errorf("unable to read file '%s': %w", keyStr, err)
	}

	return res, nil
}

func (r *ProtoReader) EstimateRowsCountOneObject(ctx context.Context, obj *aws_s3.Object) (uint64, error) {
	res, err := r.estimateRows(ctx, []*aws_s3.Object{obj})
	if err != nil {
		return 0, xerrors.Errorf("failed to estimate rows of file: %s : %w", *obj.Key, err)
	}
	return res, nil
}

func (r *ProtoReader) EstimateRowsCountAllObjects(ctx context.Context) (uint64, error) {
	files, err := s3util.ListFiles(r.bucket, r.pathPrefix, r.pathPattern, r.client, r.logger, nil, r.ObjectsFilter())
	if err != nil {
		return 0, xerrors.Errorf("unable to load file list: %w", err)
	}

	res, err := r.estimateRows(ctx, files)
	if err != nil {
		return 0, xerrors.Errorf("failed to estimate total rows: %w", err)
	}
	return res, nil
}

func (r *ProtoReader) Read(ctx context.Context, filePath string, pusher chunk_pusher.Pusher) error {
	s3RawReader, err := r.newS3RawReader(ctx, filePath)
	if err != nil {
		return xerrors.Errorf("unable to open reader: %w", err)
	}

	if s3RawReader.Size() > defaultBlockSize {
		chunkReader := abstract_reader.NewChunkReader(s3RawReader, abstract_reader.DefaultChunkReaderBlockSize, r.logger)
		defer chunkReader.Close()
		return r.streamParseFile(ctx, filePath, chunkReader, pusher, s3RawReader.LastModified())
	}

	fullFile, err := s3raw.ReadWholeFile(ctx, s3RawReader, r.blockSize)
	if err != nil {
		return xerrors.Errorf("unable to read whole file: %w", err)
	}
	msg := constructMessage(s3RawReader.LastModified(), fullFile, []byte(filePath))
	parser, err := r.parserBuilder.BuildLazyParser(msg, abstract.NewPartition(filePath, 0))
	if err != nil {
		return xerrors.Errorf("unable to prepare parser: %w", err)
	}

	var buff []abstract.ChangeItem
	buffSize := int64(0)

	for item := parser.Next(); item != nil; item = parser.Next() {
		if r.unparsedPolicy == s3.UnparsedPolicyFail {
			if err := parsers.VerifyUnparsed(*item); err != nil {
				return abstract.NewFatalError(xerrors.Errorf("unable to parse: %w", err))
			}
		}
		buff = append(buff, *item)
		buffSize += int64(item.Size.Read)
		if item.Size.Read == 0 {
			r.logger.Warn("Got item with 0 raw read size")
			buffSize += 64 * humanize.KiByte
		}
		if buffSize > perPushBatchSize {
			if err := pusher.Push(ctx, chunk_pusher.Chunk{
				Items:     buff,
				FilePath:  filePath,
				Offset:    0,
				Completed: false,
				Size:      buffSize,
			}); err != nil {
				return xerrors.Errorf("unable to push batch: %w", err)
			}
			buff = nil
			buffSize = 0
		}
	}

	if len(buff) > 0 {
		if err := pusher.Push(ctx, chunk_pusher.Chunk{
			Items:     buff,
			FilePath:  filePath,
			Offset:    0,
			Completed: false,
			Size:      buffSize,
		}); err != nil {
			return xerrors.Errorf("unable to push last batch: %w", err)
		}
	}
	return nil
}

func (r *ProtoReader) ParsePassthrough(chunk chunk_pusher.Chunk) []abstract.ChangeItem {
	// the most complex and useful method in the world
	return chunk.Items
}

func (r *ProtoReader) ResolveSchema(ctx context.Context) (*abstract.TableSchema, error) {
	if r.tableSchema != nil && len(r.tableSchema.Columns()) != 0 {
		return r.tableSchema, nil
	}

	files, err := s3util.ListFiles(r.bucket, r.pathPrefix, r.pathPattern, r.client, r.logger, aws.Int(1), r.ObjectsFilter())
	if err != nil {
		return nil, xerrors.Errorf("unable to load file list: %w", err)
	}

	if len(files) < 1 {
		return nil, xerrors.Errorf("unable to resolve schema, no files found: %s", r.pathPrefix)
	}

	return r.resolveSchema(ctx, *files[0].Key)
}

func (r *ProtoReader) ObjectsFilter() abstract_reader.ObjectsFilter {
	return abstract_reader.IsNotEmpty
}

func (r *ProtoReader) resolveSchema(ctx context.Context, key string) (*abstract.TableSchema, error) {
	s3RawReader, err := r.newS3RawReader(ctx, key)
	if err != nil {
		return nil, xerrors.Errorf("unable to open reader for file: %s: %w", key, err)
	}

	chunkReader := abstract_reader.NewChunkReader(s3RawReader, int(r.blockSize), r.logger)
	defer chunkReader.Close()
	err = chunkReader.ReadNextChunk()
	if err != nil && !xerrors.Is(err, io.EOF) {
		return nil, xerrors.Errorf("failed to read sample from file: %s: %w", key, err)
	}
	if len(chunkReader.Data()) == 0 {
		// read nothing, file was empty
		return nil, xerrors.New(fmt.Sprintf("could not read sample data from file: %s", key))
	}

	reader := bufio.NewReader(bytes.NewReader(chunkReader.Data()))
	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, xerrors.Errorf("failed to read sample content for schema deduction: %w", err)
	}
	parser, err := r.parserBuilder.BuildLazyParser(constructMessage(s3RawReader.LastModified(), content, []byte(key)), abstract.NewPartition(key, 0))
	if err != nil {
		return nil, xerrors.Errorf("failed to prepare parser: %w", err)
	}
	item := parser.Next()
	if item == nil {
		return nil, xerrors.Errorf("unable to parse sample data: %v", util.Sample(string(content), 1024))
	}
	r.tableSchema = item.TableSchema
	return r.tableSchema, nil
}

func (r *ProtoReader) streamParseFile(ctx context.Context, filePath string, chunkReader *abstract_reader.ChunkReader, pusher chunk_pusher.Pusher, lastModified time.Time) error {
	lastUnparsedData := make([]byte, 0)
	parser := r.parserBuilder.BuildBaseParser()
	for !chunkReader.IsEOF() {
		if ctx.Err() != nil {
			r.logger.Infof("stream parse file %s canceled", filePath)
			break
		}
		if err := chunkReader.ReadNextChunk(); err != nil {
			return xerrors.Errorf("failed to read sample from file: %s: %w", filePath, err)
		}
		data := chunkReader.Data()
		parsed := parser.Do(constructMessage(lastModified, data, []byte(filePath)), abstract.NewPartition(filePath, 0))
		if len(parsed) == 0 {
			continue
		}
		if unparsed := parsed[len(parsed)-1]; parsers.IsUnparsed(unparsed) {
			lastUnparsedData = data[len(data)-int(unparsed.Size.Read):]
			parsed = parsed[:len(parsed)-1]
		} else {
			lastUnparsedData = nil
		}
		parsedDataSize := int64(len(data) - len(lastUnparsedData))
		if r.unparsedPolicy == s3.UnparsedPolicyFail {
			if err := parsers.VerifyUnparsed(parsed...); err != nil {
				return abstract.NewFatalError(xerrors.Errorf("unable to parse: %w", err))
			}
		}
		if err := abstract_reader.FlushChunk(ctx, filePath, uint64(chunkReader.Offset()), parsedDataSize, parsed, pusher); err != nil {
			return xerrors.Errorf("unable to push: %w", err)
		}
		chunkReader.FillBuffer(lastUnparsedData)
	}

	if len(lastUnparsedData) > 0 && r.unparsedPolicy == s3.UnparsedPolicyFail {
		return abstract.NewFatalError(xerrors.Errorf("unparsed data found in the end of file: %s", filePath))
	}

	if len(lastUnparsedData) > 0 {
		data := chunkReader.Data()
		parsed := parser.Do(constructMessage(lastModified, data, []byte(filePath)), abstract.NewPartition(filePath, 0))
		if err := abstract_reader.FlushChunk(ctx, filePath, uint64(chunkReader.Offset()), int64(len(data)), parsed, pusher); err != nil {
			return xerrors.Errorf("unable to push: %w", err)
		}
	}

	return nil
}

func constructMessage(curTime time.Time, buff []byte, key []byte) parsers.Message {
	return parsers.Message{
		Offset:     0,
		SeqNo:      0,
		Key:        key,
		CreateTime: curTime,
		WriteTime:  curTime,
		Value:      buff,
		Headers:    nil,
	}
}

func newReaderImpl(src *s3.S3Source, lgr log.Logger, sess *session.Session, metrics *stats.SourceStats, parserBuilder parsers.ParserBuilder) (*ProtoReader, error) {
	reader := &ProtoReader{
		table: abstract.TableID{
			Namespace: src.TableNamespace,
			Name:      src.TableName,
		},
		bucket:         src.Bucket,
		client:         aws_s3.New(sess),
		downloader:     s3manager.NewDownloader(sess),
		logger:         lgr,
		tableSchema:    abstract.NewTableSchema(src.OutputSchema),
		pathPrefix:     src.PathPrefix,
		blockSize:      defaultBlockSize,
		pathPattern:    src.PathPattern,
		metrics:        metrics,
		parserBuilder:  parserBuilder,
		unparsedPolicy: src.UnparsedPolicy,
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
