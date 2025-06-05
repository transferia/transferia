package sink

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_provider "github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/serializer"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/xlocale"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/sync/semaphore"
)

type Sink struct {
	client              *s3.S3
	cfg                 *s3_provider.S3Destination
	snapshots           map[string]map[string]*snapshotHolder
	logger              log.Logger
	uploader            *s3manager.Uploader
	metrics             *stats.SinkerStats
	replicationUploader *replicationUploader
	mu                  sync.Mutex
}

func (s *Sink) Close() error {
	return nil
}

func (s *Sink) initSnapshotLoaderIfNotInited(fullTableName string, bucket string, bufferFile string, key *string) error {
	snapshotHolders, ok := s.snapshots[bufferFile]
	if !ok {
		return nil
	}
	snapshotHolder, ok := snapshotHolders[bucket]
	if ok {
		return nil
	}
	var err error
	snapshotHolder, err = s.createSnapshotIOHolder()
	if err != nil {
		return xerrors.Errorf("unable to init snapshot holder :%v:%w", fullTableName, err)
	}
	snapshotHolders[bucket] = snapshotHolder

	go func() {
		s.logger.Info("start uploading table part", log.String("table", fullTableName), log.String("key", *key))
		res, err := s.uploader.Upload(&s3manager.UploadInput{
			Body:   snapshotHolder.snapshot,
			Bucket: aws.String(s.cfg.Bucket),
			Key:    key,
		})
		s.logger.Info("upload result", log.String("table", fullTableName), log.String("key", *key), log.Any("res", res), log.Error(err))
		snapshotHolder.uploadDone <- err
		close(snapshotHolder.uploadDone)
	}()
	return nil
}

// Push to s3 looks different for replication and snaphot transfers:
//   - replications have only items with InsertKind kind items
//   - snapshots have initial InitTableLoad kind item, then
//     go items with InsertKind kind following by DoneTableLoads
//     kind item
func (s *Sink) Push(input []abstract.ChangeItem) error {
	buckets := map[string]map[string]*FileCache{}
	for i := range input {
		row := &input[i]
		bucket := s.bucket(*row)
		key := s.bucketKey(*row)
		bufferFile := s.rowPart(*row)
		switch row.Kind {
		case abstract.InsertKind:
			if err := s.initSnapshotLoaderIfNotInited(s.fqtn(row), bucket, bufferFile, key); err != nil {
				return xerrors.Errorf("unable to init snapshot loader: %w", err)
			}
			if _, ok := buckets[bucket]; !ok {
				buckets[bucket] = map[string]*FileCache{}
			}
			buffers := buckets[bucket]
			if _, ok := buffers[bufferFile]; !ok {
				buffers[bufferFile] = newFileCache(row.TableID())
			}
			_ = buffers[bufferFile].Add(row) // rows with different TableId goes to different bufferFiles in rowPart
			s.metrics.Table(s.fqtn(row), "rows", 1)
		case abstract.TruncateTableKind:
			s.logger.Info("truncate table", log.String("table", s.fqtn(row)))
			fallthrough
		case abstract.DropTableKind:
			s.logger.Info("drop table", log.String("table", s.fqtn(row)))
			res, err := s.client.DeleteObject(&s3.DeleteObjectInput{
				Bucket: aws.String(s.cfg.Bucket),
				Key:    key,
			})
			if err != nil {
				return xerrors.Errorf("unable to delete:%v:%w", key, err)
			}
			s.logger.Info("delete object res", log.Any("res", res))
		case abstract.InitShardedTableLoad:
			// not needed for now
		case abstract.InitTableLoad:
			fullTableName := s.fqtn(row)
			s.logger.Info("init table load", log.String("table", fullTableName))
			s.snapshots[bufferFile] = make(map[string]*snapshotHolder)
		case abstract.DoneTableLoad:
			fullTableName := s.fqtn(row)
			snapshots := s.snapshots[bufferFile]
			s.logger.Info("finishing uploading table", log.String("table", fullTableName))
			for _, holder := range snapshots {
				holder.snapshot.Close()
				if err := <-holder.uploadDone; err != nil {
					return xerrors.Errorf("unable to finish uploading table %q: %w", fullTableName, err)
				}
			}
			s.logger.Info("done uploading table", log.String("table", fullTableName))
		case abstract.DoneShardedTableLoad:
			// not needed for now
		case abstract.DDLKind,
			abstract.PgDDLKind,
			abstract.MongoCreateKind,
			abstract.MongoRenameKind,
			abstract.MongoDropKind,
			abstract.ChCreateTableDistributedKind,
			abstract.ChCreateTableKind:
			s.logger.Warnf("kind: %s not supported, skip", row.Kind)
		default:
			return xerrors.Errorf("kind: %v not supported", row.Kind)
		}
	}

	for bucket, fileCaches := range buckets {
		for filename, cache := range fileCaches {
			// Process snapshots
			if snapshotHoldersInBucket, ok := s.snapshots[filename]; ok {
				snapshotHolder, ok := snapshotHoldersInBucket[bucket]
				if !ok {
					s.logger.Warnf("snapshot holder not fund for %s/%s", bucket, filename)
					return xerrors.Errorf("snapshot holder not fund for %s/%s", bucket, filename)
				}
				data, err := snapshotHolder.serializer.Serialize(cache.items)
				if err != nil {
					return xerrors.Errorf("unable to upload table %s/%s: %w", bucket, filename, err)
				}
				s.logger.Info(
					"write bytes for snapshot",
					log.Int("input_length", len(input)),
					log.Int("serialized_length", len(data)),
					log.String("bucket", bucket),
					log.String("table", cache.tableID.Fqtn()),
				)

				select {
				case snapshotHolder.snapshot.FeedChannel() <- data:
				case err := <-snapshotHolder.uploadDone:
					if err != nil {
						return abstract.NewFatalError(xerrors.Errorf("unable to upload table %s/%s: %w", bucket, filename, err))
					}
				}
			} else {
				if cache.IsSnapshotFileCache() {
					s.logger.Warnf("sink: no InitTableLoad event for %s", filename)
					return xerrors.Errorf("sink: no InitTableLoad event for %s", filename)
				}
				// Process replications
				filePath := fmt.Sprintf("%s/%s", bucket, filename)
				s.logger.Info(
					"sink: items for replication",
					log.Int("input_length", len(input)),
					log.UInt64("from", cache.minLSN),
					log.UInt64("to", cache.maxLSN),
					log.String("filepath", filePath),
					log.String("table", cache.tableID.Fqtn()),
				)

				if err := s.tryUploadWithIntersectionGuard(cache, filePath); err != nil {
					return xerrors.Errorf("unable to upload buffer parts: %w", err)
				}
			}
		}
	}
	return nil
}

// S3 sink deduplication logic is based on three assumptions:
//  1. Sink is not thread safe, push is called from one thread only
//  2. for each filepath, for each push this conditions are valid:
//     - P[i].from >= P[i-1].from (equals for retries or crash)
//     - P[i].from <= P[i-1].to + 1  (equals if there are no retries)
//  3. items lsns are coherent for each file
func (s *Sink) tryUploadWithIntersectionGuard(cache *FileCache, filePath string) error {
	newBaseRange := NewObjectRange(cache.LSNRange())

	intervals := []ObjectRange{newBaseRange}
	cacheParts := cache.Split(intervals, uint64(s.cfg.BufferSize))

	sem := semaphore.NewWeighted(s.cfg.Concurrency)
	resCh := make([]chan error, len(cacheParts))

	for i, part := range cacheParts {
		batchSerializer, err := createSerializer(s.cfg)
		if err != nil {
			return xerrors.Errorf("unable to upload file part: %w", err)
		}
		data, err := batchSerializer.Serialize(part.items)
		if err != nil {
			return xerrors.Errorf("unable to upload file part: %w", err)
		}

		resCh[i] = make(chan error, 1)
		go func(i int, part *FileCache) {
			_ = sem.Acquire(context.Background(), 1)
			defer sem.Release(1)
			resCh[i] <- s.replicationUploader.Upload(filePath, part.ExtractLsns(), data)
		}(i, part)
	}
	isFatal := false
	var errs util.Errors
	for i := 0; i < len(cacheParts); i++ {
		err := <-resCh[i]
		if err != nil {
			errs = append(errs, err)
		}
		if abstract.IsFatal(err) {
			isFatal = true
		}
	}

	if len(errs) > 0 {
		if isFatal {
			return abstract.NewFatalError(xerrors.Errorf("fatal error in upload file part: %w", errs))
		}
		return xerrors.Errorf("unable to upload file part: %w", errs)
	}
	return nil
}

func (s *Sink) bucket(row abstract.ChangeItem) string {
	rowBucketTime := time.Unix(0, int64(row.CommitTime))
	if s.cfg.LayoutColumn != "" {
		rowBucketTime = model.ExtractTimeCol(row, s.cfg.LayoutColumn)
	}
	if s.cfg.LayoutTZ != "" {
		loc, _ := xlocale.Load(s.cfg.LayoutTZ)
		rowBucketTime = rowBucketTime.In(loc)
	}
	return rowBucketTime.Format(s.cfg.Layout)
}

func (s *Sink) bucketKey(row abstract.ChangeItem) *string {
	fileName := s.fqtn(&row)
	bucketKey := aws.String(fmt.Sprintf("%s/%s.%s", s.bucket(row), fileName, strings.ToLower(string(s.cfg.OutputFormat))))

	if s.cfg.OutputEncoding == s3_provider.GzipEncoding {
		bucketKey = aws.String(*bucketKey + ".gz")
	}
	return bucketKey
}

func (s *Sink) fqtn(row *abstract.ChangeItem) string {
	res := row.TableID().Name
	if row.TableID().Namespace != "" {
		res = fmt.Sprintf("%v_%v", row.TableID().Namespace, row.TableID().Name)
	}
	return res
}

func (s *Sink) rowPart(row abstract.ChangeItem) string {
	if row.IsMirror() {
		return fmt.Sprintf("%v_%v", row.ColumnValues[abstract.RawDataColsIDX[abstract.RawMessageTopic]], row.ColumnValues[abstract.RawDataColsIDX[abstract.RawMessagePartition]])
	}
	res := s.fqtn(&row)
	if row.PartID != "" {
		res = fmt.Sprintf("%s_%s", res, hashLongPart(row.PartID, 24))
	}
	return res
}

func (s *Sink) UpdateOutputFormat(f model.ParsingFormat) {
	s.cfg.OutputFormat = f
}

func (s *Sink) createSnapshotIOHolder() (*snapshotHolder, error) {
	uploadDone := make(chan error)
	var snapshot Snapshot
	if s.cfg.OutputEncoding == s3_provider.GzipEncoding {
		snapshot = NewSnapshotGzip()
	} else {
		snapshot = NewSnapshotRaw()
	}
	batchSerializer, err := createSerializer(s.cfg)
	if err != nil {
		return nil, err
	}

	return &snapshotHolder{
		uploadDone: uploadDone,
		snapshot:   snapshot,
		serializer: batchSerializer,
	}, nil
}

func createSerializer(cfg *s3_provider.S3Destination) (serializer.BatchSerializer, error) {
	switch cfg.OutputFormat {
	case model.ParsingFormatRaw:
		return serializer.NewRawBatchSerializer(
			&serializer.RawBatchSerializerConfig{
				SerializerConfig: &serializer.RawSerializerConfig{
					AddClosingNewLine: true,
				},
				BatchConfig: nil,
			},
		), nil
	case model.ParsingFormatJSON:
		return serializer.NewJSONBatchSerializer(
			&serializer.JSONBatchSerializerConfig{
				SerializerConfig: &serializer.JSONSerializerConfig{
					AddClosingNewLine:    true,
					UnsupportedItemKinds: nil,
					AnyAsString:          cfg.AnyAsString,
				},
				BatchConfig: nil,
			},
		), nil
	case model.ParsingFormatCSV:
		return serializer.NewCsvBatchSerializer(nil), nil
	case model.ParsingFormatPARQUET:
		return serializer.NewParquetBatchSerializer(), nil
	default:
		return nil, xerrors.New("s3_sink: Unsupported format")
	}
}

func hashLongPart(text string, maxLen int) string {
	if len(text) < maxLen {
		return text
	}
	return util.Hash(text)
}

func NewSink(lgr log.Logger, cfg *s3_provider.S3Destination, mtrcs metrics.Registry, cp coordinator.Coordinator, transferID string) (*Sink, error) {
	sess, err := s3_provider.NewAWSSession(lgr, cfg.Bucket, cfg.ConnectionConfig())
	if err != nil {
		return nil, xerrors.Errorf("unable to create session to s3 bucket: %w", err)
	}

	buffer := &replicationUploader{
		cfg:      cfg,
		logger:   log.With(lgr, log.Any("sub_component", "uploader")),
		uploader: s3manager.NewUploader(sess),
	}

	s3Client := s3.New(sess)
	uploader := s3manager.NewUploader(sess)
	uploader.PartSize = cfg.PartSize

	return &Sink{
		client:              s3Client,
		cfg:                 cfg,
		logger:              lgr,
		metrics:             stats.NewSinkerStats(mtrcs),
		uploader:            uploader,
		mu:                  sync.Mutex{},
		snapshots:           map[string]map[string]*snapshotHolder{},
		replicationUploader: buffer,
	}, nil
}
