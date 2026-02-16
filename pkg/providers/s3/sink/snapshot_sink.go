package sink

import (
	"context"
	"io"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_provider "github.com/transferia/transferia/pkg/providers/s3"
	s3_writer "github.com/transferia/transferia/pkg/providers/s3/sink/writer"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

type SnapshotSink struct {
	transferID         string
	operationTimestamp string
	s3Client           s3Client
	cfg                *s3_provider.S3Destination
	snapshotWriter     *snapshotWriter
	logger             log.Logger
	metrics            *stats.SinkerStats
	fileSplitter       *FileSplitter
}

func (s *SnapshotSink) Close() error {
	s.logger.Info("closing snapshot sink")
	return s.snapshotWriter.close()
}

func (s *SnapshotSink) Push(input []abstract.ChangeItem) error {
	insertItems, err := s.processItemsAndReturnInserts(input)
	if err != nil {
		return xerrors.Errorf("unable to push item: %w", err)
	}
	if err := s.processSnapshot(insertItems); err != nil {
		return xerrors.Errorf("unable to process buckets: %w", err)
	}
	rowFqtn := rowFqtn(input[0].TableID())
	s.metrics.Table(rowFqtn, "rows", len(insertItems))

	return nil
}

func (s *SnapshotSink) initSnapshotLoaderIfNotInited(fullTableName string, ref s3ObjectRef) error {
	if s.snapshotWriter != nil {
		return nil
	}
	keyPartNumber := s.fileSplitter.keyNumber(ref)

	return s.initPipe(fullTableName, ref, keyPartNumber)
}

func (s *SnapshotSink) initPipe(fullTableName string, ref s3ObjectRef, keyPartNumber int) error {
	pipeReader, pipeWriter := io.Pipe()
	key := s.fileSplitter.increaseKey(ref)
	s.logger.Infof("init pipe for %s", key)

	batchSerializer, err := createSerializer(s.cfg.OutputFormat, s.cfg.AnyAsString)
	if err != nil {
		return xerrors.Errorf("unable to create serializer with outputFormat: %s: %w", s.cfg.OutputFormat, err)
	}
	writer := s3_writer.NewWriter(s.cfg.OutputEncoding, pipeWriter)
	snapshotWriter, err := newsnapshotWriter(
		context.Background(),
		batchSerializer,
		writer,
		key,
	)
	if err != nil {
		return xerrors.Errorf("unable to create snapshot writer: %w", err)
	}

	s.snapshotWriter = snapshotWriter
	go func() {
		defer pipeReader.Close()
		s.logger.Info("start uploading table part", log.String("table", fullTableName), log.String("key", key))

		uploadInput := &s3manager.UploadInput{
			Body:   pipeReader,
			Bucket: aws.String(s.cfg.Bucket),
			Key:    aws.String(key),
			Metadata: map[string]*string{
				"debug-data-transfer.file-encoding": aws.String(string(s.cfg.OutputEncoding)),
				"debug-data-transfer.file-format":   aws.String(string(s.cfg.OutputFormat)),
				"debug-data-transfer.key-part":      aws.String(strconv.Itoa(keyPartNumber)),
				"debug-data-transfer.part-id":       aws.String(ref.partID),
				"debug-data-transfer.source-name":   aws.String(fullTableName),
				"debug-data-transfer.transfer-id":   aws.String(s.transferID),
			},
		}

		res, err := s.s3Client.Upload(uploadInput)
		if err != nil {
			err = pipeReader.CloseWithError(xerrors.Errorf("unable to upload table part: %w", err))
		} else {
			err = pipeReader.Close()
		}
		snapshotWriter.finishUpload(err)
		s.logger.Info("upload result", log.String("table", fullTableName), log.String("key", key), log.Any("res", res), log.Error(err))
	}()

	return nil
}

func (s *SnapshotSink) processItemsAndReturnInserts(input []abstract.ChangeItem) ([]*abstract.ChangeItem, error) {
	insertItemsLen := countInsertItems(input)

	insertItems := make([]*abstract.ChangeItem, 0, insertItemsLen)
	for _, row := range input {
		fullTableName := rowFqtn(row.TableID())
		switch row.Kind {
		case abstract.InsertKind:
			insertItems = append(insertItems, &row)
		case abstract.TruncateTableKind:
			s.logger.Info("truncate table", log.String("table", fullTableName))
			fallthrough
		case abstract.DropTableKind:
			ref := s.makeS3ObjectRef(row)
			key := s.fileSplitter.key(ref)
			s.logger.Info("drop table", log.String("table", fullTableName))
			res, err := s.s3Client.DeleteObject(&s3.DeleteObjectInput{
				Bucket: aws.String(s.cfg.Bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				return nil, xerrors.Errorf("unable to delete:%v:%w", key, err)
			}
			s.logger.Info("delete object res", log.Any("res", res))
		case abstract.InitShardedTableLoad, abstract.DoneShardedTableLoad:
			s.logger.Infof("init/done sharded table load: %s", fullTableName)
		case abstract.InitTableLoad:
			s.logger.Info("init table load", log.String("table", fullTableName))
		case abstract.DoneTableLoad:
			s.logger.Info("finishing uploading table", log.String("table", fullTableName))
			if err := s.doneTableLoad(); err != nil {
				return nil, xerrors.Errorf("unable to finish uploading table %q: %w", fullTableName, err)
			}
			s.logger.Info("done uploading table", log.String("table", fullTableName))
		case abstract.DDLKind,
			abstract.PgDDLKind,
			abstract.MongoCreateKind,
			abstract.MongoRenameKind,
			abstract.MongoDropKind,
			abstract.ChCreateTableKind:
			s.logger.Warnf("kind: %s not supported, skip", row.Kind)
		default:
			return nil, xerrors.Errorf("kind: %v not supported", row.Kind)
		}
	}
	return insertItems, nil
}

func (s *SnapshotSink) doneTableLoad() error {
	if err := s.snapshotWriter.close(); err != nil {
		return xerrors.Errorf("unable to close snapshot holder: %w", err)
	}
	s.snapshotWriter = nil
	return nil
}

// processSnapshot processes the snapshot of the insert items from one table
func (s *SnapshotSink) processSnapshot(insertItems []*abstract.ChangeItem) error {
	if len(insertItems) == 0 {
		return nil
	}

	ref := s.makeS3ObjectRef(*insertItems[0])
	table := insertItems[0].TableID().Fqtn()
	if err := s.initSnapshotLoaderIfNotInited(table, ref); err != nil {
		return xerrors.Errorf("unable to init snapshot loader: %w", err)
	}

	remainingItems := insertItems
	for len(remainingItems) > 0 {
		if s.snapshotWriter == nil {
			return xerrors.Errorf("snapshot holder not found for %s", s.fileSplitter.key(ref))
		}

		written, err := s.writeChunkAndRotate(ref, table, remainingItems)
		if err != nil {
			return xerrors.Errorf("unable to write chunk and rotate: %w", err)
		}
		remainingItems = remainingItems[written:]
	}

	return nil
}

// writeChunkAndRotate writes as many items as the current file allows, then
// rotates to a new file if limits were hit. Returns the number of items consumed.
func (s *SnapshotSink) writeChunkAndRotate(ref s3ObjectRef, table string, items []*abstract.ChangeItem) (int, error) {
	rowsToWrite := s.fileSplitter.addItems(ref, items)

	if rowsToWrite > 0 {
		if err := s.writeBatch(items[:rowsToWrite], table); err != nil {
			return 0, xerrors.Errorf("unable to write batch: %w", err)
		}
	}

	// All items fit into the current file — no rotation needed
	if rowsToWrite == len(items) {
		return rowsToWrite, nil
	}

	// Current file reached its limit — rotate to a new one
	if err := s.rotateFile(ref, table); err != nil {
		return 0, xerrors.Errorf("unable to rotate file: %w", err)
	}

	return rowsToWrite, nil
}

// writeBatch serializes and writes items to the current snapshot file.
func (s *SnapshotSink) writeBatch(items []*abstract.ChangeItem, table string) error {
	writtenBytes, err := s.snapshotWriter.write(items)
	if err != nil {
		return xerrors.Errorf("unable to write data to snapshot: %w", err)
	}
	s.logger.Info(
		"wrote bytes for snapshot",
		log.Int("input_length", len(items)),
		log.Int("written_bytes", writtenBytes),
		log.String("table", table),
	)
	return nil
}

// rotateFile closes the current snapshot file and opens a new one with an incremented counter.
func (s *SnapshotSink) rotateFile(ref s3ObjectRef, table string) error {
	s.logger.Infof("rotating file for %s", s.fileSplitter.key(ref))

	if err := s.snapshotWriter.close(); err != nil {
		return xerrors.Errorf("unable to close snapshot holder: %w", err)
	}

	keyPartNumber := s.fileSplitter.keyNumber(ref)
	if err := s.initPipe(table, ref, keyPartNumber); err != nil {
		return xerrors.Errorf("unable to init pipe for new file %s: %w", s.fileSplitter.key(ref), err)
	}

	return nil
}

// makeS3ObjectRef creates an s3ObjectRef from a ChangeItem using the source table's
// namespace and name, the operation timestamp, and the item's partID.
func (s *SnapshotSink) makeS3ObjectRef(row abstract.ChangeItem) s3ObjectRef {
	return newS3ObjectRef(
		s.cfg.Layout,
		row.TableID().Namespace,
		row.TableID().Name,
		row.PartID,
		s.operationTimestamp,
		s.cfg.OutputFormat,
		s.cfg.OutputEncoding,
	)
}

func (s *SnapshotSink) UpdateOutputFormat(f model.ParsingFormat) {
	s.cfg.OutputFormat = f
}

func NewSnapshotSink(
	lgr log.Logger,
	cfg *s3_provider.S3Destination,
	mtrcs metrics.Registry,
	cp coordinator.Coordinator,
	transferID string,
	operationTimestamp int64,
) (*SnapshotSink, error) {
	sess, err := s3_provider.NewAWSSession(lgr, cfg.Bucket, cfg.ConnectionConfig())
	if err != nil {
		return nil, xerrors.Errorf("unable to create session to s3 bucket: %w", err)
	}

	s3Client := s3.New(sess)
	uploader := s3manager.NewUploader(sess)
	uploader.PartSize = cfg.PartSize

	return &SnapshotSink{
		transferID:         transferID,
		operationTimestamp: strconv.FormatInt(operationTimestamp, 10),
		s3Client:           newS3ClientImpl(s3Client, uploader),
		cfg:                cfg,
		logger:             lgr,
		metrics:            stats.NewSinkerStats(mtrcs),
		snapshotWriter:     nil,
		fileSplitter:       newFileSplitter(cfg.MaxItemsPerFile, cfg.MaxBytesPerFile),
	}, nil
}
