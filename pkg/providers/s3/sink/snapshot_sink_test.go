package sink

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/format"
	s3_provider "github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/s3recipe"
	"github.com/transferia/transferia/pkg/providers/s3/sink/testutil"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/yt/go/schema"
)

func generateBullets(table string, count int) []abstract.ChangeItem {
	var res []abstract.ChangeItem
	for i := 0; i < count; i++ {
		res = append(res, abstract.ChangeItem{
			Kind:         abstract.InsertKind,
			CommitTime:   uint64(time.Now().UnixNano()),
			Table:        table,
			TableSchema:  abstract.NewTableSchema([]abstract.ColSchema{{DataType: string(schema.TypeString)}, {DataType: string(schema.TypeString)}}),
			ColumnValues: []interface{}{fmt.Sprintf("test1_value_%v", i), fmt.Sprintf("test2_value_%v", i)},
		})
	}
	return res
}

//
// Tests
//

func TestS3Sink(t *testing.T) {
	t.Run("testS3SinkUploadTable", testS3SinkUploadTable)
	t.Run("testS3SinkUploadTableGzip", testS3SinkUploadTableGzip)
	t.Run("testJsonSnapshot", testJsonSnapshot)
	t.Run("testRotationParquet", testRotationParquet)
}

func testS3SinkUploadTable(t *testing.T) {
	cfg := s3recipe.PrepareS3(t, "TestS3SinkUploadTable", model.ParsingFormatCSV, s3_provider.GzipEncoding)
	cp := testutil.NewFakeClientWithTransferState()
	currSink, err := NewSnapshotSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, "TestS3SinkUploadTable", 0)
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

func testS3SinkUploadTableGzip(t *testing.T) {
	cfg := s3recipe.PrepareS3(t, "TestS3SinkUploadTableGzip", model.ParsingFormatCSV, s3_provider.GzipEncoding)

	cp := testutil.NewFakeClientWithTransferState()
	currSink, err := NewSnapshotSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, "TestS3SinkUploadTableGzip", 0)
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

	sess, err := s3_provider.NewAWSSession(logger.Log, cfg.Bucket, cfg.ConnectionConfig())
	require.NoError(t, err)
	s3Client := s3.New(sess)

	// Build expected key using the sink's operation timestamp
	expectedKey := fmt.Sprintf("%s/test_table/part-%s-%s.00000.csv.gz", cfg.Layout, currSink.operationTimestamp, hashPartID(""))
	obj, err := s3Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(expectedKey),
	})
	defer func() {
		require.NoError(t, currSink.Push([]abstract.ChangeItem{
			{Kind: abstract.DropTableKind, CommitTime: uint64(time.Now().UnixNano()), Table: "test_table"},
		}))
	}()
	require.NoError(t, err, fmt.Sprintf("expected key: %s", expectedKey))
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

func testJsonSnapshot(t *testing.T) {
	bucket := "testjsonnoencode"
	cfg := s3recipe.PrepareS3(t, bucket, model.ParsingFormatJSON, s3_provider.NoEncoding)
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

			currSink, err := NewSnapshotSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, "TestJSONSnapshot", 0)
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

			sess, err := s3_provider.NewAWSSession(logger.Log, cfg.Bucket, cfg.ConnectionConfig())
			require.NoError(t, err)
			s3Client := s3.New(sess)

			expectedKey := fmt.Sprintf("%s/%v/part-%s-%s.00000.json", cfg.Layout, table, currSink.operationTimestamp, hashPartID(""))
			obj, err := s3Client.GetObject(&s3.GetObjectInput{
				Bucket: aws.String(cfg.Bucket),
				Key:    aws.String(expectedKey),
			})
			require.NoError(t, err)

			data, err := io.ReadAll(obj.Body)
			require.NoError(t, err)
			require.Equal(t, string(data), tc.expectedResult, fmt.Sprintf("expected result: %s, got: %s", tc.expectedResult, string(data)))
		})
	}
}

func testRotationParquet(t *testing.T) {
	cfg := &s3_provider.S3Destination{
		OutputFormat:   model.ParsingFormatPARQUET,
		OutputEncoding: s3_provider.NoEncoding,
		BufferSize:     1 * 1024 * 1024,
		BufferInterval: time.Second * 5,
		Bucket:         "testRotationParquet",
	}
	mockS3Client := testutil.NewMockS3Client()

	testTimestamp := "1700000000"
	currSink := &SnapshotSink{
		s3Client:           mockS3Client,
		cfg:                cfg,
		operationTimestamp: testTimestamp,
		snapshotWriter:     nil,
		logger:             logger.Log,
		metrics:            stats.NewSinkerStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
		fileSplitter:       newFileSplitter(1, 0),
	}

	require.NoError(t, currSink.Push([]abstract.ChangeItem{
		{Kind: abstract.InitTableLoad, CommitTime: uint64(time.Now().UnixNano()), Table: "test_table"},
	}))

	require.NoError(t, currSink.Push(generateBullets("test_table", 10)))
	require.NoError(t, currSink.Push([]abstract.ChangeItem{
		{Kind: abstract.DoneTableLoad, CommitTime: uint64(time.Now().UnixNano()), Table: "test_table"},
	}))

	defaultHash := hashPartID("")
	require.Len(t, mockS3Client.BucketFiles[cfg.Bucket], 10)

	expectedFirstKey := fmt.Sprintf("test_table/part-%s-%s.00000.parquet", testTimestamp, defaultHash)
	expectedData, ok := mockS3Client.BucketFiles[cfg.Bucket][expectedFirstKey]
	require.True(t, ok, "expected key %s not found in bucket files: %v", expectedFirstKey, mockS3Client.BucketFiles[cfg.Bucket])

	for i := range 9 {
		key := fmt.Sprintf("test_table/part-%s-%s.%05d.parquet", testTimestamp, defaultHash, i+1)
		data, ok := mockS3Client.BucketFiles[cfg.Bucket][key]
		require.True(t, ok, "expected key %s not found", key)
		require.Equal(t, expectedData, data)
	}
}
