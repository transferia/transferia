package reader

import (
	"context"
	"io"
	"math"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem/strictify"
	"github.com/transferia/transferia/pkg/parsers/scanner"
	"github.com/transferia/transferia/pkg/providers/s3"
	chunk_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
)

var (
	_ Reader     = (*LineReader)(nil)
	_ RowCounter = (*LineReader)(nil)
)

type LineReader struct {
	table          abstract.TableID
	bucket         string
	client         s3iface.S3API
	downloader     *s3manager.Downloader
	logger         log.Logger
	metrics        *stats.SourceStats
	tableSchema    *abstract.TableSchema
	fastCols       abstract.FastTableSchema
	batchSize      int
	blockSize      int64
	pathPrefix     string
	pathPattern    string
	ColumnNames    []string
	hideSystemCols bool
}

func (r *LineReader) TotalRowCount(ctx context.Context) (uint64, error) {
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

func (r *LineReader) RowCount(ctx context.Context, obj *aws_s3.Object) (uint64, error) {
	res, err := r.estimateRows(ctx, []*aws_s3.Object{obj})
	if err != nil {
		return 0, xerrors.Errorf("failed to estimate rows of file: %s : %w", *obj.Key, err)
	}
	return res, nil
}

func (r *LineReader) estimateRows(ctx context.Context, files []*aws_s3.Object) (uint64, error) {
	res := uint64(0)

	totalSize, sampleReader, err := estimateTotalSize(ctx, r.logger, files, r.openReader)
	if err != nil {
		return 0, xerrors.Errorf("unable to estimate rows: %w", err)
	}

	if totalSize > 0 && sampleReader != nil {
		data := make([]byte, r.blockSize)
		bytesRead, err := sampleReader.ReadAt(data, 0)
		if err != nil && !xerrors.Is(err, io.EOF) {
			return uint64(0), xerrors.Errorf("failed to estimate row count: %w", err)
		}

		if bytesRead > 0 {
			lines, bytesRead, err := readLines(data)
			if err != nil {
				return uint64(0), xerrors.Errorf("failed to estimate row count: %w", err)
			}
			bytesPerLine := float64(bytesRead) / float64(len(lines))
			totalLines := math.Ceil(float64(totalSize) / bytesPerLine)
			res = uint64(totalLines)
		}
	}

	return res, nil
}

func (r *LineReader) openReader(ctx context.Context, filePath string) (*S3Reader, error) {
	sr, err := NewS3Reader(ctx, r.client, r.downloader, r.bucket, filePath, r.metrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to create reader at: %w", err)
	}

	return sr, nil
}

func (r *LineReader) Read(ctx context.Context, filePath string, pusher chunk_pusher.Pusher) error {
	s3Reader, err := r.openReader(ctx, filePath)
	if err != nil {
		return xerrors.Errorf("unable to open reader: %w", err)
	}

	offset := 0
	lineCounter := uint64(1)
	var readBytes int
	var lines []string

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("Read canceled")
			return nil
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
				return xerrors.Errorf("failed to read from file: %w", err)
			}
		}

		lines, readBytes, err = readLines(data)
		if err != nil {
			return xerrors.Errorf("failed to read lines from file: %w", err)
		}

		offset += readBytes
		var buff []abstract.ChangeItem
		var currentSize int64

		for _, line := range lines {
			if len(strings.TrimSpace(line)) == 0 {
				continue
			}

			ci, err := r.doParse(line, filePath, s3Reader.LastModified(), lineCounter)
			if err != nil {
				continue
			}

			lineCounter++
			buff = append(buff, *ci)

			if len(buff) > r.batchSize {
				if err := pusher.Push(ctx, chunk_pusher.Chunk{
					Items:     buff,
					FilePath:  filePath,
					Offset:    lineCounter,
					Completed: lastRound,
					Size:      currentSize,
				}); err != nil {
					return xerrors.Errorf("unable to push: %w", err)
				}
				currentSize = 0
				buff = []abstract.ChangeItem{}
			}
		}

		if len(buff) > 0 {
			if err := pusher.Push(ctx, chunk_pusher.Chunk{
				Items:     buff,
				FilePath:  filePath,
				Offset:    lineCounter,
				Completed: lastRound,
				Size:      currentSize,
			}); err != nil {
				return xerrors.Errorf("unable to push: %w", err)
			}
		}

		if lastRound {
			break
		}
	}

	return nil
}

func (r *LineReader) doParse(line string, filePath string, lastModified time.Time, lineCounter uint64) (*abstract.ChangeItem, error) {
	ci, err := r.constructCI(line, filePath, lastModified, lineCounter)
	if err != nil {
		return nil, xerrors.Errorf("unable to construct change item: %w", err)
	}

	if err := strictify.Strictify(ci, r.fastCols); err != nil {
		return nil, xerrors.Errorf("failed to convert value to the expected data type: %w", err)
	}

	return ci, nil
}

func (r *LineReader) constructCI(line string, fname string, lastModified time.Time, idx uint64) (*abstract.ChangeItem, error) {
	values := make([]interface{}, len(r.tableSchema.Columns()))
	columnIndex := 0
	if !r.hideSystemCols {
		values[columnIndex] = fname
		columnIndex++
		values[columnIndex] = idx
		columnIndex++
	}
	values[columnIndex] = line

	return &abstract.ChangeItem{
		ID:           0,
		LSN:          0,
		CommitTime:   uint64(lastModified.UnixNano()),
		Counter:      0,
		Kind:         abstract.InsertKind,
		Schema:       r.table.Namespace,
		Table:        r.table.Name,
		PartID:       fname,
		ColumnNames:  r.ColumnNames,
		ColumnValues: values,
		TableSchema:  r.tableSchema,
		OldKeys:      abstract.EmptyOldKeys(),
		TxID:         "",
		Query:        "",
		Size:         abstract.RawEventSize(util.DeepSizeof(values)),
	}, nil
}

func readLines(content []byte) ([]string, int, error) {
	scanner := scanner.NewLineBreakScanner(content)
	scannedLines, err := scanner.ScanAll()
	if err != nil {
		return nil, 0, xerrors.Errorf("failed to split all read lines: %w", err)
	}
	bytesRead := 0

	for _, scannedLine := range scannedLines {
		bytesRead += (len(scannedLine) + len("\n"))
	}

	return scannedLines, bytesRead, nil
}

func (r *LineReader) ParsePassthrough(chunk chunk_pusher.Chunk) []abstract.ChangeItem {
	return chunk.Items
}

func (r *LineReader) ObjectsFilter() ObjectsFilter { return IsNotEmpty }

func (r *LineReader) ResolveSchema(ctx context.Context) (*abstract.TableSchema, error) {
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

	return abstract.NewTableSchema([]abstract.ColSchema{abstract.NewColSchema("row", schema.TypeBytes, false)}), nil
}

func NewLineReader(src *s3.S3Source, lgr log.Logger, sess *session.Session, metrics *stats.SourceStats) (*LineReader, error) {
	reader := &LineReader{
		table: abstract.TableID{
			Namespace: src.TableNamespace,
			Name:      src.TableName,
		},
		bucket:         src.Bucket,
		client:         aws_s3.New(sess),
		downloader:     s3manager.NewDownloader(sess),
		logger:         lgr,
		metrics:        metrics,
		tableSchema:    abstract.NewTableSchema(src.OutputSchema),
		fastCols:       abstract.NewTableSchema(src.OutputSchema).FastColumns(),
		batchSize:      0,
		blockSize:      1 * 1024 * 1024, // 1mb,
		pathPrefix:     src.PathPrefix,
		pathPattern:    src.PathPattern,
		ColumnNames:    nil,
		hideSystemCols: src.HideSystemCols,
	}

	var err error

	// only one column exists
	reader.tableSchema, err = reader.ResolveSchema(context.Background())
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve schema: %w", err)
	}

	// append system columns at the end if necessary
	if !reader.hideSystemCols {
		cols := reader.tableSchema.Columns()
		reader.tableSchema = appendSystemColsTableSchema(cols)
	}

	reader.ColumnNames = yslices.Map(reader.tableSchema.Columns(), func(t abstract.ColSchema) string { return t.ColumnName })

	return reader, nil
}
