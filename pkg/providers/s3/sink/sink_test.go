package sink

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/format"
	s3_provider "github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/sink/testutil"
	"go.ytsaurus.tech/yt/go/schema"
)

func canonFile(t *testing.T, client *s3.S3, bucket, file string) {
	obj, err := client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(file),
	})
	require.NoError(t, err)
	data, err := io.ReadAll(obj.Body)
	require.NoError(t, err)
	logger.Log.Infof("read data: %v", format.SizeInt(len(data)))
	unzipped, err := gzip.NewReader(bytes.NewReader(data))
	require.NoError(t, err)
	unzippedData, err := io.ReadAll(unzipped)
	require.NoError(t, err)
	logger.Log.Infof("unpack data: %v", format.SizeInt(len(unzippedData)))
	logger.Log.Infof("%s content:\n%s", file, string(unzippedData))
	t.Run(fmt.Sprintf("%s_%s", t.Name(), file), func(t *testing.T) {
		canon.SaveJSON(t, string(unzippedData))
	})
}

func cleanup(t *testing.T, currSink *Sink, cfg *s3_provider.S3Destination, objects *s3.ListObjectsOutput) {
	if os.Getenv("S3_ACCESS_KEY") == "" {
		return
	}

	var toDelete []*s3.ObjectIdentifier
	for _, obj := range objects.Contents {
		toDelete = append(toDelete, &s3.ObjectIdentifier{Key: obj.Key})
	}
	res, err := currSink.client.DeleteObjects(&s3.DeleteObjectsInput{
		Bucket: aws.String(cfg.Bucket),
		Delete: &s3.Delete{
			Objects: toDelete,
			Quiet:   nil,
		},
	})
	logger.Log.Infof("delete: %v", res)
	require.NoError(t, err)
}

var timeBulletSchema = abstract.NewTableSchema([]abstract.ColSchema{
	{ColumnName: "logical_time", DataType: schema.TypeTimestamp.String()},
	{ColumnName: "test1", DataType: schema.TypeString.String()},
	{ColumnName: "test2", DataType: schema.TypeString.String()},
})

func generateTimeBucketBullets(logicalTime time.Time, table string, l, r int, partID string) []abstract.ChangeItem {
	var res []abstract.ChangeItem
	for i := l; i <= r; i++ {
		res = append(res, abstract.ChangeItem{
			LSN:          uint64(i),
			Kind:         abstract.InsertKind,
			CommitTime:   uint64(time.Now().UnixNano()),
			Table:        table,
			PartID:       partID,
			ColumnNames:  []string{"logical_time", "test1", "test2"},
			ColumnValues: []interface{}{logicalTime, fmt.Sprintf("test1_value_%v", i), fmt.Sprintf("test2_value_%v", i)},
			TableSchema:  timeBulletSchema,
		})
	}
	return res
}

func generateRawMessages(table string, part, from, to int) []abstract.ChangeItem {
	ciTime := time.Date(2022, time.Month(10), 19, 0, 0, 0, 0, time.UTC)
	var res []abstract.ChangeItem
	for i := from; i < to; i++ {
		res = append(res, abstract.MakeRawMessage(
			table,
			ciTime,
			"test-topic",
			part,
			int64(i),
			[]byte(fmt.Sprintf("test_part_%v_value_%v", part, i)),
		))
	}
	return res
}

//
// Tests
//

func TestS3SinkUploadTable(t *testing.T) {
	cfg := s3_provider.PrepareS3(t, "TestS3SinkUploadTable", model.ParsingFormatCSV, s3_provider.GzipEncoding)
	cfg.Layout = "e2e_test-2006-01-02"
	cp := testutil.NewFakeClientWithTransferState()
	currSink, err := NewSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, "TestS3SinkUploadTable")
	require.NoError(t, err)
	require.NoError(t, currSink.Push([]abstract.ChangeItem{
		{Kind: abstract.InitTableLoad, CommitTime: uint64(time.Now().UnixNano()), Table: "test_table"},
	}))

	require.NoError(t, currSink.Push(generateBullets("test_table", 50000)))
	require.NoError(t, currSink.Push(generateBullets("test_table", 50000)))
	require.NoError(t, currSink.Push(generateBullets("test_table", 50000)))
	require.NoError(t, currSink.Push(generateBullets("test_table", 50000)))
	require.NoError(t, currSink.Push([]abstract.ChangeItem{
		{Kind: abstract.DoneTableLoad, CommitTime: uint64(time.Now().UnixNano()), Table: "test_table"},
	}))
	require.NoError(t, currSink.Push([]abstract.ChangeItem{
		{Kind: abstract.DropTableKind, CommitTime: uint64(time.Now().UnixNano()), Table: "test_table"},
	}))
}

func TestS3SinkBucketTZ(t *testing.T) {
	cfg := s3_provider.PrepareS3(t, "TestS3SinkBucketTZ", model.ParsingFormatCSV, s3_provider.GzipEncoding)
	cfg.Layout = "02 Jan 06 15:04 MST"
	cfg.LayoutTZ = "CET"

	cp := testutil.NewFakeClientWithTransferState()
	currSink, err := NewSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, "TestS3SinkBucketTZ")
	require.NoError(t, err)
	b := currSink.bucket(abstract.ChangeItem{Kind: abstract.DoneTableLoad, CommitTime: uint64(time.Date(2022, time.Month(10), 19, 0, 0, 0, 0, time.UTC).UnixNano()), Table: "test_table"})
	require.Equal(t, "19 Oct 22 02:00 CEST", b)

	cfg.LayoutTZ = "UTC"
	b = currSink.bucket(abstract.ChangeItem{Kind: abstract.DoneTableLoad, CommitTime: uint64(time.Date(2022, time.Month(10), 19, 0, 0, 0, 0, time.UTC).UnixNano()), Table: "test_table"})
	require.Equal(t, "19 Oct 22 00:00 UTC", b)
}

func TestS3SinkUploadTableGzip(t *testing.T) {
	cfg := s3_provider.PrepareS3(t, "TestS3SinkUploadTableGzip", model.ParsingFormatCSV, s3_provider.GzipEncoding)
	cfg.Layout = "test_gzip"

	cp := testutil.NewFakeClientWithTransferState()
	currSink, err := NewSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, "TestS3SinkUploadTableGzip")
	require.NoError(t, err)
	require.NoError(t, currSink.Push([]abstract.ChangeItem{
		{Kind: abstract.InitTableLoad, CommitTime: uint64(time.Now().UnixNano()), Table: "test_table"},
	}))

	require.NoError(t, currSink.Push(generateBullets("test_table", 50000)))
	require.NoError(t, currSink.Push(generateBullets("test_table", 50000)))
	require.NoError(t, currSink.Push(generateBullets("test_table", 50000)))
	require.NoError(t, currSink.Push(generateBullets("test_table", 50000)))
	require.NoError(t, currSink.Push([]abstract.ChangeItem{
		{Kind: abstract.DoneTableLoad, CommitTime: uint64(time.Now().UnixNano()), Table: "test_table"},
	}))
	require.NoError(t, currSink.Close())
	obj, err := currSink.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String("test_gzip/test_table.csv.gz"),
	})
	defer func() {
		require.NoError(t, currSink.Push([]abstract.ChangeItem{
			{Kind: abstract.DropTableKind, CommitTime: uint64(time.Now().UnixNano()), Table: "test_table"},
		}))
	}()
	require.NoError(t, err)
	data, err := io.ReadAll(obj.Body)
	require.NoError(t, err)
	logger.Log.Infof("read data: %v", format.SizeInt(len(data)))
	require.True(t, len(data) > 0)
	unzipped, err := gzip.NewReader(bytes.NewReader(data))
	require.NoError(t, err)
	unzippedData, err := io.ReadAll(unzipped)
	require.NoError(t, err)
	logger.Log.Infof("unpack data: %v", format.SizeInt(len(unzippedData)))
	require.Len(t, unzippedData, 7111120)
}

func generateBullets(table string, count int) []abstract.ChangeItem {
	var res []abstract.ChangeItem
	for i := 0; i < count; i++ {
		res = append(res, abstract.ChangeItem{
			Kind:         abstract.InsertKind,
			CommitTime:   uint64(time.Now().UnixNano()),
			Table:        table,
			ColumnNames:  []string{"test1", "test2"},
			ColumnValues: []interface{}{fmt.Sprintf("test1_value_%v", i), fmt.Sprintf("test2_value_%v", i)},
		})
	}
	return res
}

func TestJsonReplication(t *testing.T) {
	bucket := "testjsonnoencode"
	cfg := s3_provider.PrepareS3(t, bucket, model.ParsingFormatJSON, s3_provider.NoEncoding)
	cfg.Layout = bucket
	cp := testutil.NewFakeClientWithTransferState()

	tests := []struct {
		objKey         string
		anyAsString    bool
		expectedResult string
	}{
		{
			objKey:         "complex_to_string",
			anyAsString:    true,
			expectedResult: "{\"object\":\"{\\\"key\\\":\\\"value\\\"}\"}\n",
		},
		{
			objKey:         "complex_as_is",
			anyAsString:    false,
			expectedResult: "{\"object\":{\"key\":\"value\"}}\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.objKey, func(t *testing.T) {
			cfg.AnyAsString = tc.anyAsString
			table := "test_table"

			currSink, err := NewSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, "TestJSONReplication")
			require.NoError(t, err)
			defer require.NoError(t, currSink.Close())

			require.NoError(t, currSink.Push([]abstract.ChangeItem{
				{Kind: abstract.InitTableLoad, CommitTime: uint64(time.Now().UnixNano()), Table: table},
			}))
			defer require.NoError(t, currSink.Push([]abstract.ChangeItem{
				{Kind: abstract.DropTableKind, CommitTime: uint64(time.Now().UnixNano()), Table: table},
			}))

			require.NoError(t, currSink.Push([]abstract.ChangeItem{
				{
					Kind:       abstract.InsertKind,
					CommitTime: uint64(time.Now().UnixNano()),
					Table:      table,
					TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
						{DataType: string(schema.TypeAny)},
					}),
					ColumnNames:  []string{"object"},
					ColumnValues: []any{map[string]string{"key": "value"}},
				},
			}))
			require.NoError(t, currSink.Push([]abstract.ChangeItem{
				{Kind: abstract.DoneTableLoad, CommitTime: uint64(time.Now().UnixNano()), Table: table},
			}))

			obj, err := currSink.client.GetObject(&s3.GetObjectInput{
				Bucket: aws.String(cfg.Bucket),
				Key:    aws.String(fmt.Sprintf("%v/%v.json", cfg.Layout, table)),
			})
			require.NoError(t, err)

			data, err := io.ReadAll(obj.Body)
			require.NoError(t, err)
			require.Equal(t, string(data), tc.expectedResult)
		})
	}
}

func TestRawReplication(t *testing.T) {
	cfg := s3_provider.PrepareS3(t, "testrawgzip", model.ParsingFormatRaw, s3_provider.GzipEncoding)
	cfg.Layout = "test_raw_gzip"
	cp := testutil.NewFakeClientWithTransferState()
	currSink, err := NewSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, "TestRawReplication")
	require.NoError(t, err)

	parts := []int{0, 1, 2, 3}
	wg := sync.WaitGroup{}
	for _, part := range parts {
		wg.Add(1)
		go func(part int) {
			defer wg.Done()
			require.NoError(t, currSink.Push(generateRawMessages("test_table", part, 0, 1000)))
		}(part)
	}
	wg.Wait()
	require.NoError(t, currSink.Close())

	for _, part := range parts {
		objKey := fmt.Sprintf("test_raw_gzip/test-topic_%v-0_999.raw.gz", part)
		t.Run(objKey, func(t *testing.T) {
			obj, err := currSink.client.GetObject(&s3.GetObjectInput{
				Bucket: aws.String(cfg.Bucket),
				Key:    aws.String(objKey),
			})
			require.NoError(t, err)
			defer require.NoError(t, currSink.Push([]abstract.ChangeItem{
				{Kind: abstract.DropTableKind, CommitTime: uint64(time.Now().UnixNano()), Table: "test_table"},
			}))
			data, err := io.ReadAll(obj.Body)
			require.NoError(t, err)
			logger.Log.Infof("read data: %v", format.SizeInt(len(data)))
			require.True(t, len(data) > 0)
			unzipped, err := gzip.NewReader(bytes.NewReader(data))
			require.NoError(t, err)
			unzippedData, err := io.ReadAll(unzipped)
			require.NoError(t, err)
			logger.Log.Infof("unpack data: %v", format.SizeInt(len(unzippedData)))
			require.Len(t, unzippedData, 21890)
		})
	}
}

func TestReplicationWithWorkerFailure(t *testing.T) {
	cfg := s3_provider.PrepareS3(t, "TestReplicationWithWorkerFailure", model.ParsingFormatCSV, s3_provider.GzipEncoding)
	cp := testutil.NewFakeClientWithTransferState()
	currSink, err := NewSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, "TestReplicationWithWorkerFailure")
	require.NoError(t, err)

	// 1 Iteration    1-10
	require.NoError(t, currSink.Push(generateRawMessages("test_table", 1, 1, 11)))
	require.NoError(t, currSink.Close())

	// 2 Iteration 1-12 upload does not work
	// simulate retry by recreating a new Sink
	currSink, err = NewSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, "TestReplicationWithWorkerFailure")
	require.NoError(t, err)

	require.NoError(t, currSink.Push(generateRawMessages("test_table", 1, 1, 13)))
	time.Sleep(5 * time.Second)

	// simulate upload failure by deleting lastly created object and adding inflight from previous push
	objKey := "2022/10/19/test-topic_1-11_12.csv.gz"
	_, err = currSink.client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(objKey),
	})

	require.NoError(t, err)
	time.Sleep(5 * time.Second)

	require.NoError(t, currSink.Close())

	// 3 Iteration retry 1-12
	// simulate retry by recreating a new Sink
	currSink, err = NewSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, "TestReplicationWithWorkerFailure")
	require.NoError(t, err)

	require.NoError(t, currSink.Push(generateRawMessages("test_table", 1, 1, 13)))

	objects, err := currSink.client.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(cfg.Bucket),
		Prefix: aws.String("2022/10/19/"),
	})
	require.NoError(t, err)
	defer cleanup(t, currSink, cfg, objects)
	require.Equal(t, 2, len(objects.Contents))
	require.Contains(t, objects.GoString(), "2022/10/19/test-topic_1-1_10.csv.gz")
	require.Contains(t, objects.GoString(), "2022/10/19/test-topic_1-1_12.csv.gz")
	require.NoError(t, currSink.Close())
}

func TestCustomColLayautFailures(t *testing.T) {
	cfg := s3_provider.PrepareS3(t, t.Name(), model.ParsingFormatCSV, s3_provider.GzipEncoding)
	cfg.LayoutColumn = "logical_time"

	cp := testutil.NewFakeClientWithTransferState()
	currSink, err := NewSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, t.Name())
	require.NoError(t, err)

	// initial push 1-10
	var round1 []abstract.ChangeItem
	day1 := time.Date(2022, time.January, 1, 0, 0, 0, 0, time.UTC)
	day2 := time.Date(2022, time.January, 2, 0, 0, 0, 0, time.UTC)
	day3 := time.Date(2022, time.January, 3, 0, 0, 0, 0, time.UTC)
	partID := "part_1"
	round1 = append(round1, generateTimeBucketBullets(day1, "test_table", 1, 3, partID)...)
	round1 = append(round1, generateTimeBucketBullets(day2, "test_table", 4, 6, partID)...)
	round1 = append(round1, generateTimeBucketBullets(day1, "test_table", 7, 8, partID)...)
	round1 = append(round1, generateTimeBucketBullets(day2, "test_table", 9, 10, partID)...)

	require.NoError(t, currSink.Push(round1))
	require.NoError(t, currSink.Close())

	// 2 Iteration 1-15 upload does not work
	// overlapping retry
	currSink, err = NewSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, t.Name())
	require.NoError(t, err)
	round2 := append(round1, generateTimeBucketBullets(day3, "test_table", 11, 13, partID)...)
	round2 = append(round2, generateTimeBucketBullets(day2, "test_table", 14, 15, partID)...)
	require.NoError(t, currSink.Push(round2))
	require.NoError(t, currSink.Close())

	// 3 Iteration 11-15 upload does not work
	// non-overlapping retry
	currSink, err = NewSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, t.Name())
	require.NoError(t, err)
	var round3 []abstract.ChangeItem
	round3 = append(round3, generateTimeBucketBullets(day3, "test_table", 11, 13, partID)...)
	round3 = append(round3, generateTimeBucketBullets(day2, "test_table", 14, 15, partID)...)
	require.NoError(t, currSink.Push(round3))
	require.NoError(t, currSink.Close())

	objects, err := currSink.client.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(cfg.Bucket),
		Prefix: aws.String("2022/01/"),
	})
	require.NoError(t, err)
	defer cleanup(t, currSink, cfg, objects)
	objKeys := yslices.Map(objects.Contents, func(t *s3.Object) string {
		return *t.Key
	})
	logger.Log.Infof("found: %s", objKeys)
	require.Len(t, objKeys, 5) // duplicated file
	require.NoError(t, currSink.Close())
}

func TestParquetReplication(t *testing.T) {
	cfg := s3_provider.PrepareS3(t, t.Name(), model.ParsingFormatPARQUET, s3_provider.GzipEncoding)
	cfg.LayoutColumn = "logical_time"

	cp := testutil.NewFakeClientWithTransferState()
	currSink, err := NewSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, t.Name())
	require.NoError(t, err)

	var round1 []abstract.ChangeItem
	day1 := time.Date(2022, time.January, 1, 0, 0, 0, 0, time.UTC)
	partID := "part_1"
	round1 = append(round1, generateTimeBucketBullets(day1, "test_table", 1, 100, partID)...)

	require.NoError(t, currSink.Push(round1))
	require.NoError(t, currSink.Close())

	objects, err := currSink.client.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(cfg.Bucket),
		Prefix: aws.String("2022/01/"),
	})
	require.NoError(t, err)
	defer cleanup(t, currSink, cfg, objects)
	objKeys := yslices.Map(objects.Contents, func(t *s3.Object) string {
		return *t.Key
	})
	logger.Log.Infof("found: %s", objKeys)
	require.Len(t, objKeys, 1)
	canonFile(t, currSink.client, cfg.Bucket, "2022/01/01/test_table_part_1-1_100.parquet.gz")
	require.NoError(t, currSink.Close())
}

func TestParquetReadAfterWrite(t *testing.T) {
	cfg := s3_provider.PrepareS3(t, t.Name(), model.ParsingFormatPARQUET, s3_provider.NoEncoding)
	cfg.LayoutColumn = "logical_time"

	cp := testutil.NewFakeClientWithTransferState()
	currSink, err := NewSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, t.Name())
	require.NoError(t, err)

	var round1 []abstract.ChangeItem
	day1 := time.Date(2022, time.January, 1, 0, 0, 0, 0, time.UTC)
	partID := "part_1"
	round1 = append(round1, generateTimeBucketBullets(day1, "test_table", 1, 100, partID)...)

	require.NoError(t, currSink.Push(round1))
	require.NoError(t, currSink.Close())
}

func TestRawReplicationHugeFiles(t *testing.T) {
	cfg := s3_provider.PrepareS3(t, "hugereplfiles", model.ParsingFormatRaw, s3_provider.NoEncoding)
	cfg.BufferSize = 5 * 1024 * 1024
	cfg.Layout = "huge_repl_files"

	cp := testutil.NewFakeClientWithTransferState()
	currSink, err := NewSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, "TestRawReplicationHugeFiles")
	require.NoError(t, err)

	parts := []int{0}
	wg := sync.WaitGroup{}
	for _, part := range parts {
		wg.Add(1)
		go func(part int) {
			defer wg.Done()
			require.NoError(t, currSink.Push(generateRawMessages("test_table", part, 1, 1_000_000)))
		}(part)
	}
	wg.Wait()
	require.NoError(t, currSink.Close())
	t.Run("verify", func(t *testing.T) {
		objects, err := currSink.client.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(cfg.Bucket),
		})
		require.NoError(t, err)
		defer cleanup(t, currSink, cfg, objects)
		require.Equal(t, 5, len(objects.Contents))
	})
}
