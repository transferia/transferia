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
	"github.com/transferia/transferia/pkg/providers/s3/s3util/s3sess"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
	s3recipe "github.com/transferia/transferia/pkg/providers/s3/v1/recipe"
	"github.com/transferia/transferia/pkg/providers/s3/v1/sink/testutil"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/yt/go/schema"
)

var testTableSchema = abstract.NewTableSchema([]abstract.ColSchema{
	abstract.NewColSchema("test1", schema.TypeString, false),
	abstract.NewColSchema("test2", schema.TypeString, false),
})

func generateBullets(table string, count int) []abstract.ChangeItem {
	var res []abstract.ChangeItem
	for i := 0; i < count; i++ {
		res = append(res, abstract.ChangeItem{
			Kind:         abstract.InsertKind,
			CommitTime:   uint64(time.Now().UnixNano()),
			Table:        table,
			ColumnValues: []interface{}{fmt.Sprintf("test1_value_%v", i), fmt.Sprintf("test2_value_%v", i)},
			ColumnNames:  []string{"test1", "test2"},
			TableSchema:  testTableSchema,
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

func newTestDropSink(t *testing.T, prefix string, seed map[string][]byte) (*SnapshotSink, *testutil.MockS3Client) {
	t.Helper()
	mock := testutil.NewMockS3Client()
	bucket := "test-bucket"
	mock.BucketFiles[bucket] = seed
	cfg := &s3_v1_model.S3Destination{
		Bucket:         bucket,
		SerializerType: model.ParsingFormatJSON,
		Serializer:     s3_v1_model.SerializerUnion{Json: &s3_v1_model.JsonSerializerConfig{Encoding: s3_v1_model.NoEncoding}},
		Prefix:         prefix,
	}
	return &SnapshotSink{
		cfg:                cfg,
		s3Client:           mock,
		logger:             logger.Log,
		metrics:            stats.NewSinkerStats(solomon.NewRegistry(nil)),
		snapshotWriter:     nil,
		fileSplitter:       newFileSplitter(cfg.MaxItemsPerFile, cfg.MaxBytesPerFile),
		serializer:         cfg.GetSerializer(),
		operationTimestamp: "1700000000",
	}, mock
}

func testS3SinkUploadTable(t *testing.T) {
	serializer := &s3_v1_model.CSVSerializerConfig{Encoding: s3_v1_model.GzipEncoding}
	cfg := s3recipe.PrepareS3(t, "TestS3SinkUploadTable", serializer)
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
	serializer := &s3_v1_model.CSVSerializerConfig{Encoding: s3_v1_model.GzipEncoding}
	cfg := s3recipe.PrepareS3(t, "TestS3SinkUploadTableGzip", serializer)

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

	sess, err := s3sess.NewAWSSession(logger.Log, cfg.Bucket, cfg.Connection)
	require.NoError(t, err)
	s3Client := s3.New(sess)

	// Build expected key using the sink's operation timestamp
	expectedKey := fmt.Sprintf("%s/test_table/part-%s-%s.00000.csv.gz", cfg.Prefix, currSink.operationTimestamp, hashPartID(""))
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
	serializer := &s3_v1_model.JsonSerializerConfig{Encoding: s3_v1_model.NoEncoding}
	cfg := s3recipe.PrepareS3(t, bucket, serializer)
	cp := testutil.NewFakeClientWithTransferState()

	tests := []struct {
		objKey         string
		expectedResult string
	}{
		{
			objKey:         "complex_as_is",
			expectedResult: "{\"object\":{\"key\":\"value\"}}\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.objKey, func(t *testing.T) {
			cfg.Serializer = s3_v1_model.SerializerUnion{Json: &s3_v1_model.JsonSerializerConfig{Encoding: s3_v1_model.NoEncoding}}
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

			sess, err := s3sess.NewAWSSession(logger.Log, cfg.Bucket, cfg.Connection)
			require.NoError(t, err)
			s3Client := s3.New(sess)

			expectedKey := fmt.Sprintf("%s/%v/part-%s-%s.00000.json", cfg.Prefix, table, currSink.operationTimestamp, hashPartID(""))
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
	cfg := &s3_v1_model.S3Destination{
		SerializerType: model.ParsingFormatPARQUET,
		Serializer:     s3_v1_model.SerializerUnion{Parquet: &s3_v1_model.ParquetSerializerConfig{}},
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
		fileSplitter:       newFileSplitter(10, 0),
		serializer:         cfg.GetSerializer(),
	}

	require.NoError(t, currSink.Push([]abstract.ChangeItem{
		{Kind: abstract.InitTableLoad, CommitTime: uint64(time.Now().UnixNano()), Table: "test_table"},
	}))

	// generate 10 same items to test rotation
	for i := 0; i < 10; i++ {
		require.NoError(t, currSink.Push(generateBullets("test_table", 10)))
	}
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

func TestSnapshotSink_DropTable_DeletesOnlyTablePrefix(t *testing.T) {
	seed := map[string][]byte{
		"prefix/ns/tbl/part-1-abcd1234.00001.json":       []byte("x"),
		"prefix/ns/tbl/part-2-deadbeef.00002.json":       []byte("x"),
		"prefix/ns/tbl2/part-1-abcd1234.00001.json":      []byte("keep"),
		"prefix/ns/other/part-1-abcd1234.00001.json":     []byte("keep"),
		"other-prefix/ns/tbl/part-1-abcd1234.00001.json": []byte("keep"),
	}
	sink, mock := newTestDropSink(t, "prefix", seed)

	err := sink.dropTable(sink.makeS3ObjectRef(abstract.ChangeItem{
		Schema: "ns",
		Table:  "tbl",
	}))
	require.NoError(t, err)

	require.Len(t, mock.BucketFiles["test-bucket"], 3)
	require.Contains(t, mock.BucketFiles["test-bucket"], "prefix/ns/tbl2/part-1-abcd1234.00001.json")
	require.Contains(t, mock.BucketFiles["test-bucket"], "prefix/ns/other/part-1-abcd1234.00001.json")
	require.Contains(t, mock.BucketFiles["test-bucket"], "other-prefix/ns/tbl/part-1-abcd1234.00001.json")
}

func TestSnapshotSink_DropTable_Pagination(t *testing.T) {
	seed := map[string][]byte{}
	const total = 2500
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("ns/tbl/part-%d-abcd1234.%05d.json", i, i)
		seed[key] = []byte("x")
	}
	sink, mock := newTestDropSink(t, "", seed)
	mock.ListPageSize = 1000

	err := sink.dropTable(sink.makeS3ObjectRef(abstract.ChangeItem{
		Schema: "ns",
		Table:  "tbl",
	}))
	require.NoError(t, err)

	require.Len(t, mock.BucketFiles["test-bucket"], 0)
	require.Len(t, mock.DeleteObjectsCalls, 3, "expected 3 batched DeleteObjects calls (1000+1000+500)")
	require.Len(t, mock.DeleteObjectsCalls[0].Delete.Objects, 1000)
	require.Len(t, mock.DeleteObjectsCalls[1].Delete.Objects, 1000)
	require.Len(t, mock.DeleteObjectsCalls[2].Delete.Objects, 500)
}

func TestSnapshotSink_DropTable_NothingToDrop(t *testing.T) {
	sink, mock := newTestDropSink(t, "", map[string][]byte{})

	err := sink.dropTable(sink.makeS3ObjectRef(abstract.ChangeItem{
		Schema: "ns",
		Table:  "tbl",
	}))
	require.NoError(t, err)
	require.Len(t, mock.DeleteObjectsCalls, 0)
}

func TestSnapshotSink_Push_DropKindInvokesDropTable(t *testing.T) {
	seed := map[string][]byte{
		"prefix/ns/tbl/part-1-abcd1234.00001.json":   []byte("x"),
		"prefix/ns/tbl/part-2-deadbeef.00002.json":   []byte("x"),
		"prefix/ns/other/part-1-abcd1234.00001.json": []byte("keep"),
	}
	sink, mock := newTestDropSink(t, "prefix", seed)

	err := sink.Push([]abstract.ChangeItem{
		{
			Kind:   abstract.DropTableKind,
			Schema: "ns",
			Table:  "tbl",
		},
	})
	require.NoError(t, err)

	require.Len(t, mock.BucketFiles["test-bucket"], 1)
	require.Contains(t, mock.BucketFiles["test-bucket"], "prefix/ns/other/part-1-abcd1234.00001.json")
	require.Len(t, mock.DeleteObjectsCalls, 1)
	require.Len(t, mock.DeleteObjectsCalls[0].Delete.Objects, 2)
}
