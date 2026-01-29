package sink

import (
	"bytes"
	"context"
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
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/xlocale"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/sync/semaphore"
)

type ReplicationSink struct {
	client              *s3.S3
	cfg                 *s3_provider.S3Destination
	logger              log.Logger
	uploader            *s3manager.Uploader
	metrics             *stats.SinkerStats
	replicationUploader *replicationUploader
}

func (s *ReplicationSink) Close() error {
	return nil
}

func (s *ReplicationSink) Push(input []abstract.ChangeItem) error {
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

func (s *ReplicationSink) pushItem(row *abstract.ChangeItem, buckets buckets) error {
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
	case abstract.InitTableLoad, abstract.DoneTableLoad:
		// ReplicationSink does not handle snapshot events
		s.logger.Warnf("ReplicationSink: ignoring %s event for table %s", row.Kind, fullTableName)
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

func (s *ReplicationSink) processBuckets(buckets buckets, inputLen int) error {
	for bucket, fileCaches := range buckets {
		for filename, cache := range fileCaches {
			if err := s.processReplications(filename, bucket, cache, inputLen); err != nil {
				return xerrors.Errorf("unable to process replication for table %s/%s: %w", bucket, filename, err)
			}
		}
	}

	return nil
}

func (s *ReplicationSink) insert(row *abstract.ChangeItem, buckets buckets) error {
	bufferFile := rowPart(*row)
	bucket := s.bucket(*row)
	rowFqtn := rowFqtn(row.TableID())
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

func (s *ReplicationSink) processReplications(filename string, bucket string, cache *FileCache, inputLen int) error {
	if cache.IsSnapshotFileCache() {
		s.logger.Warnf("sink: no InitTableLoad event for %s", filename)
		return xerrors.Errorf("sink: no InitTableLoad event for %s", filename)
	}
	// Process replications
	filePath := fmt.Sprintf("%s/%s", bucket, filename)
	s.logger.Info(
		"sink: items for replication",
		log.Int("input_length", inputLen),
		log.UInt64("from", cache.minLSN),
		log.UInt64("to", cache.maxLSN),
		log.String("filepath", filePath),
		log.String("table", cache.tableID.Fqtn()),
	)

	if err := s.tryUploadWithIntersectionGuard(cache, filePath); err != nil {
		return xerrors.Errorf("unable to upload buffer parts: %w", err)
	}

	return nil
}

// S3 sink deduplication logic is based on three assumptions:
//  1. Sink is not thread safe, push is called from one thread only
//  2. for each filepath, for each push this conditions are valid:
//     - P[i].from >= P[i-1].from (equals for retries or crash)
//     - P[i].from <= P[i-1].to + 1  (equals if there are no retries)
//  3. items lsns are coherent for each file
func (s *ReplicationSink) tryUploadWithIntersectionGuard(cache *FileCache, filePath string) error {
	newBaseRange := NewObjectRange(cache.LSNRange())

	intervals := []ObjectRange{newBaseRange}
	cacheParts := cache.Split(intervals, uint64(s.cfg.BufferSize))

	sem := semaphore.NewWeighted(s.cfg.Concurrency)
	resCh := make([]chan error, len(cacheParts))

	for i, part := range cacheParts {
		data, err := s.serialize(part)
		if err != nil {
			return xerrors.Errorf("unable to serialize part: %w", err)
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

func (s *ReplicationSink) serialize(part *FileCache) ([]byte, error) {
	batchSerializer, err := createSerializer(s.cfg.OutputFormat, s.cfg.AnyAsString)
	if err != nil {
		return nil, xerrors.Errorf("unable to upload file part: %w", err)
	}
	data, err := batchSerializer.Serialize(part.items)
	if err != nil {
		return nil, xerrors.Errorf("unable to upload file part: %w", err)
	}
	buffer := bytes.NewBuffer(make([]byte, 0, len(data)))
	buffer.Write(data)
	remainingData, err := batchSerializer.Close()
	if err != nil {
		return nil, xerrors.Errorf("unable to close batch serializer: %w", err)
	}
	buffer.Write(remainingData)

	return buffer.Bytes(), nil
}

func (s *ReplicationSink) bucket(row abstract.ChangeItem) string {
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

func (s *ReplicationSink) bucketKey(row abstract.ChangeItem) *string {
	fileName := rowFqtn(row.TableID())
	bucketKey := aws.String(fmt.Sprintf("%s/%s.%s", s.bucket(row), fileName, strings.ToLower(string(s.cfg.OutputFormat))))

	if s.cfg.OutputEncoding == s3_provider.GzipEncoding {
		bucketKey = aws.String(*bucketKey + ".gz")
	}
	return bucketKey
}

func (s *ReplicationSink) UpdateOutputFormat(f model.ParsingFormat) {
	s.cfg.OutputFormat = f
}

func NewReplicationSink(lgr log.Logger, cfg *s3_provider.S3Destination, mtrcs metrics.Registry, cp coordinator.Coordinator, transferID string) (*ReplicationSink, error) {
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

	return &ReplicationSink{
		client:              s3Client,
		cfg:                 cfg,
		logger:              lgr,
		metrics:             stats.NewSinkerStats(mtrcs),
		uploader:            uploader,
		replicationUploader: buffer,
	}, nil
}
