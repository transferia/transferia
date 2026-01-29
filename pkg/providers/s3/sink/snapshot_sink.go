package sink

import (
	"fmt"
	"strings"
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
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util/xlocale"
	"go.ytsaurus.tech/library/go/core/log"
)

type SnapshotSink struct {
	client    *s3.S3
	cfg       *s3_provider.S3Destination
	snapshots map[string]map[string]*snapshotHolder
	logger    log.Logger
	uploader  *s3manager.Uploader
	metrics   *stats.SinkerStats
}

func (s *SnapshotSink) Close() error {
	return nil
}

func (s *SnapshotSink) Push(input []abstract.ChangeItem) error {
	buckets := buckets{}
	for i := range input {
		if err := s.pushItem(&input[i], buckets); err != nil {
			return xerrors.Errorf("unable to push item: %w", err)
		}
	}
	if err := s.processBuckets(buckets, len(input)); err != nil {
		return xerrors.Errorf("unable to process buckets: %w", err)
	}

	return nil
}

func (s *SnapshotSink) initSnapshotLoaderIfNotInited(fullTableName string, bucket string, bufferFile string, key *string) error {
	snapshotHolders, ok := s.snapshots[bufferFile]
	if !ok {
		return nil
	}
	snapshotHolder, ok := snapshotHolders[bucket]
	if ok {
		return nil
	}
	snapshotHolder, err := createSnapshotIOHolder(s.cfg.OutputEncoding, s.cfg.OutputFormat, s.cfg.AnyAsString)
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

func (s *SnapshotSink) pushItem(row *abstract.ChangeItem, buckets buckets) error {
	fullTableName := rowFqtn(row.TableID())
	switch row.Kind {
	case abstract.InsertKind:
		if err := s.insert(row, buckets); err != nil {
			return xerrors.Errorf("unable to insert: %w", err)
		}
	case abstract.TruncateTableKind:
		s.logger.Info("truncate table", log.String("table", fullTableName))
		fallthrough
	case abstract.DropTableKind:
		key := s.bucketKey(*row)
		s.logger.Info("drop table", log.String("table", fullTableName))
		res, err := s.client.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(s.cfg.Bucket),
			Key:    key,
		})
		if err != nil {
			return xerrors.Errorf("unable to delete:%v:%w", key, err)
		}
		s.logger.Info("delete object res", log.Any("res", res))
	case abstract.InitShardedTableLoad, abstract.DoneShardedTableLoad:
		// not needed for now
	case abstract.InitTableLoad:
		s.logger.Info("init table load", log.String("table", fullTableName))
		s.snapshots[rowPart(*row)] = make(map[string]*snapshotHolder)
	case abstract.DoneTableLoad:
		snapshots := s.snapshots[rowPart(*row)]
		s.logger.Info("finishing uploading table", log.String("table", fullTableName))
		for _, holder := range snapshots {
			lastBytes, err := holder.serializer.Close()
			if err != nil {
				return xerrors.Errorf("unable to close serializer: %w", err)
			}
			holder.snapshot.FeedChannel() <- lastBytes
			holder.snapshot.Close()
			if err := <-holder.uploadDone; err != nil {
				return xerrors.Errorf("unable to finish uploading table %q: %w", fullTableName, err)
			}
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
		return xerrors.Errorf("kind: %v not supported", row.Kind)
	}
	return nil
}

func (s *SnapshotSink) processBuckets(buckets buckets, inputLen int) error {
	for bucket, fileCaches := range buckets {
		for filename, cache := range fileCaches {
			// Process snapshots
			if snapshotHoldersInBucket, ok := s.snapshots[filename]; ok {
				if err := s.processSnapshot(filename, bucket, cache, inputLen, snapshotHoldersInBucket); err != nil {
					return xerrors.Errorf("unable to process snapshot for table %s/%s: %w", bucket, filename, err)
				}
			} else {
				s.logger.Warnf("snapshot holder not fund for %s/%s", bucket, filename)
				return xerrors.Errorf("snapshot holder not fund for %s/%s", bucket, filename)
			}
		}
	}

	return nil
}

func (s *SnapshotSink) insert(row *abstract.ChangeItem, buckets buckets) error {
	bufferFile := rowPart(*row)
	bucket := s.bucket(*row)
	key := s.bucketKey(*row)
	rowFqtn := rowFqtn(row.TableID())
	if err := s.initSnapshotLoaderIfNotInited(rowFqtn, bucket, bufferFile, key); err != nil {
		return xerrors.Errorf("unable to init snapshot loader: %w", err)
	}
	if _, ok := buckets[bucket]; !ok {
		buckets[bucket] = map[string]*FileCache{}
	}
	buffers := buckets[bucket]
	var buffer *FileCache
	if existingBuffer, ok := buffers[bufferFile]; !ok {
		buffer = newFileCache(row.TableID())
		buffers[bufferFile] = buffer
	} else {
		buffer = existingBuffer
	}
	_ = buffer.Add(row) // rows with different TableId goes to different bufferFiles in rowPart
	s.metrics.Table(rowFqtn, "rows", 1)

	return nil
}

func (s *SnapshotSink) processSnapshot(filename string, bucket string, cache *FileCache, inputLen int, snapshotHoldersInBucket map[string]*snapshotHolder) error {
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
		log.Int("input_length", inputLen),
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

	return nil
}

func (s *SnapshotSink) bucket(row abstract.ChangeItem) string {
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

func (s *SnapshotSink) bucketKey(row abstract.ChangeItem) *string {
	fileName := rowFqtn(row.TableID())
	bucketKey := aws.String(fmt.Sprintf("%s/%s.%s", s.bucket(row), fileName, strings.ToLower(string(s.cfg.OutputFormat))))

	if s.cfg.OutputEncoding == s3_provider.GzipEncoding {
		bucketKey = aws.String(*bucketKey + ".gz")
	}
	return bucketKey
}

func (s *SnapshotSink) UpdateOutputFormat(f model.ParsingFormat) {
	s.cfg.OutputFormat = f
}

func NewSnapshotSink(lgr log.Logger, cfg *s3_provider.S3Destination, mtrcs metrics.Registry, cp coordinator.Coordinator, transferID string) (*SnapshotSink, error) {
	sess, err := s3_provider.NewAWSSession(lgr, cfg.Bucket, cfg.ConnectionConfig())
	if err != nil {
		return nil, xerrors.Errorf("unable to create session to s3 bucket: %w", err)
	}

	s3Client := s3.New(sess)
	uploader := s3manager.NewUploader(sess)
	uploader.PartSize = cfg.PartSize

	return &SnapshotSink{
		client:    s3Client,
		cfg:       cfg,
		logger:    lgr,
		metrics:   stats.NewSinkerStats(mtrcs),
		uploader:  uploader,
		snapshots: map[string]map[string]*snapshotHolder{},
	}, nil
}
