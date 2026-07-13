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
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"github.com/transferia/transferia/pkg/providers/s3/s3recipe"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/s3sess"
	"github.com/transferia/transferia/pkg/providers/s3/sink/testutil"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/yt/go/schema"
)

var testTableSchema = abstract.NewTableSchema([]abstract.ColSchema{
	abstract.NewColSchema("test1", schema.TypeString, false),
	abstract.NewColSchema("test2", schema.TypeString, false),
})

func generateBullets(table string, count int) []abstract.ChangeItem {
	return generateBulletsAt(table, count, time.Now().UTC())
}

func generateBulletsAt(table string, count int, commitTime time.Time) []abstract.ChangeItem {
	var res []abstract.ChangeItem
	for i := 0; i < count; i++ {
		res = append(res, abstract.ChangeItem{
			Kind:         abstract.InsertKind,
			CommitTime:   uint64(commitTime.UnixNano()),
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
	t.Run("testDateLayoutSnapshot", testDateLayoutSnapshot)
	t.Run("testLiteralLayoutSnapshot", testLiteralLayoutSnapshot)
	t.Run("testRotationParquet", testRotationParquet)
	t.Run("testSnapshotSinkDropPolicy", testSnapshotSinkDropPolicy)
}

func testS3SinkUploadTable(t *testing.T) {
	cfg := s3recipe.PrepareS3(t, "TestS3SinkUploadTable", model.ParsingFormatCSV, s3_model.GzipEncoding)
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
	cfg := s3recipe.PrepareS3(t, "TestS3SinkUploadTableGzip", model.ParsingFormatCSV, s3_model.GzipEncoding)
	cfg.LayoutTZ = "UTC"
	fixedTime := time.Date(2024, time.February, 3, 10, 20, 30, 0, time.UTC)

	cp := testutil.NewFakeClientWithTransferState()
	currSink, err := NewSnapshotSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, "TestS3SinkUploadTableGzip", 0)
	require.NoError(t, err)
	require.NoError(t, currSink.Push([]abstract.ChangeItem{
		{Kind: abstract.InitTableLoad, CommitTime: uint64(time.Now().UnixNano()), Table: "test_table"},
	}))

	require.NoError(t, currSink.Push(generateBulletsAt("test_table", 50000, fixedTime)))
	require.NoError(t, currSink.Push(generateBulletsAt("test_table", 50000, fixedTime)))
	require.NoError(t, currSink.Push(generateBulletsAt("test_table", 50000, fixedTime)))
	require.NoError(t, currSink.Push(generateBulletsAt("test_table", 50000, fixedTime)))
	require.NoError(t, currSink.Push([]abstract.ChangeItem{
		{Kind: abstract.DoneTableLoad, CommitTime: uint64(time.Now().UnixNano()), Table: "test_table"},
	}))
	require.NoError(t, currSink.Close())

	sess, err := s3sess.NewAWSSession(logger.Log, cfg.Bucket, cfg.ConnectionConfig())
	require.NoError(t, err)
	s3Client := s3.New(sess)

	// Build expected key using the sink's operation timestamp
	expectedKey := fmt.Sprintf("%s/test_table/part-%s-%s.00000.csv.gz", fixedTime.Format(cfg.Layout), currSink.operationTimestamp, hashPartID(""))
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
	cfg := s3recipe.PrepareS3(t, bucket, model.ParsingFormatJSON, s3_model.NoEncoding)
	cfg.LayoutTZ = "UTC"
	fixedTime := time.Date(2024, time.March, 4, 5, 6, 7, 0, time.UTC)
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
					CommitTime: uint64(fixedTime.UnixNano()),
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

			sess, err := s3sess.NewAWSSession(logger.Log, cfg.Bucket, cfg.ConnectionConfig())
			require.NoError(t, err)
			s3Client := s3.New(sess)

			expectedKey := fmt.Sprintf("%s/%v/part-%s-%s.00000.json", fixedTime.Format(cfg.Layout), table, currSink.operationTimestamp, hashPartID(""))
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

func testDateLayoutSnapshot(t *testing.T) {
	cfg := &s3_model.S3Destination{
		OutputFormat:   model.ParsingFormatJSON,
		OutputEncoding: s3_model.NoEncoding,
		Layout:         "2006/01/02",
		LayoutTZ:       "UTC",
		Bucket:         "testDateLayoutSnapshot",
	}
	mockS3Client := testutil.NewMockS3Client()

	currSink := &SnapshotSink{
		s3Client:           mockS3Client,
		cfg:                cfg,
		operationTimestamp: "1700000000",
		snapshotWriters:    make(map[S3ObjectRef]*SnapshotWriter),
		logger:             logger.Log,
		metrics:            stats.NewSinkerStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
		fileSplitter:       newFileSplitter(0, 0),
	}

	day1 := time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC)
	day2 := day1.Add(24 * time.Hour)
	table := "test_table"

	require.NoError(t, currSink.Push([]abstract.ChangeItem{
		{Kind: abstract.InitTableLoad, CommitTime: uint64(day1.UnixNano()), Table: table},
	}))
	require.NoError(t, currSink.Push([]abstract.ChangeItem{
		{
			Kind:         abstract.InsertKind,
			CommitTime:   uint64(day1.UnixNano()),
			Table:        table,
			TableSchema:  testTableSchema,
			ColumnNames:  []string{"test1", "test2"},
			ColumnValues: []any{"value1", "value2"},
		},
		{
			Kind:         abstract.InsertKind,
			CommitTime:   uint64(day2.UnixNano()),
			Table:        table,
			TableSchema:  testTableSchema,
			ColumnNames:  []string{"test1", "test2"},
			ColumnValues: []any{"value3", "value4"},
		},
	}))
	require.NoError(t, currSink.Push([]abstract.ChangeItem{
		{Kind: abstract.DoneTableLoad, CommitTime: uint64(day2.UnixNano()), Table: table},
	}))

	defaultHash := hashPartID("")
	day1Key := fmt.Sprintf("%s/%s/part-%s-%s.00000.json", day1.Format(cfg.Layout), table, currSink.operationTimestamp, defaultHash)
	day2Key := fmt.Sprintf("%s/%s/part-%s-%s.00000.json", day2.Format(cfg.Layout), table, currSink.operationTimestamp, defaultHash)

	require.Contains(t, mockS3Client.BucketFiles[cfg.Bucket], day1Key)
	require.Contains(t, mockS3Client.BucketFiles[cfg.Bucket], day2Key)
	require.Equal(t, "{\"test1\":\"value1\",\"test2\":\"value2\"}\n", string(mockS3Client.BucketFiles[cfg.Bucket][day1Key]))
	require.Equal(t, "{\"test1\":\"value3\",\"test2\":\"value4\"}\n", string(mockS3Client.BucketFiles[cfg.Bucket][day2Key]))
}

func testLiteralLayoutSnapshot(t *testing.T) {
	cfg := s3recipe.PrepareS3(t, "testliterallayoutsnapshot", model.ParsingFormatJSON, s3_model.NoEncoding)
	cfg.Layout = "snapshot-prefix"
	cfg.LayoutTZ = "UTC"
	cp := testutil.NewFakeClientWithTransferState()

	currSink, err := NewSnapshotSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, "TestLiteralLayoutSnapshot", 0)
	require.NoError(t, err)
	defer require.NoError(t, currSink.Close())

	table := "test_table"
	require.NoError(t, currSink.Push([]abstract.ChangeItem{
		{Kind: abstract.InitTableLoad, CommitTime: uint64(time.Now().UnixNano()), Table: table},
	}))
	defer require.NoError(t, currSink.Push([]abstract.ChangeItem{
		{Kind: abstract.DropTableKind, CommitTime: uint64(time.Now().UnixNano()), Table: table},
	}))

	require.NoError(t, currSink.Push([]abstract.ChangeItem{
		{
			Kind:         abstract.InsertKind,
			CommitTime:   uint64(time.Now().UnixNano()),
			Table:        table,
			TableSchema:  testTableSchema,
			ColumnNames:  []string{"test1", "test2"},
			ColumnValues: []any{"value1", "value2"},
		},
	}))
	require.NoError(t, currSink.Push([]abstract.ChangeItem{
		{Kind: abstract.DoneTableLoad, CommitTime: uint64(time.Now().UnixNano()), Table: table},
	}))

	sess, err := s3sess.NewAWSSession(logger.Log, cfg.Bucket, cfg.ConnectionConfig())
	require.NoError(t, err)
	s3Client := s3.New(sess)

	expectedKey := fmt.Sprintf("snapshot-prefix/%v/part-%s-%s.00000.json", table, currSink.operationTimestamp, hashPartID(""))
	obj, err := s3Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(expectedKey),
	})
	require.NoError(t, err)

	data, err := io.ReadAll(obj.Body)
	require.NoError(t, err)
	require.Equal(t, "{\"test1\":\"value1\",\"test2\":\"value2\"}\n", string(data))
}

func testRotationParquet(t *testing.T) {
	cfg := &s3_model.S3Destination{
		OutputFormat:   model.ParsingFormatPARQUET,
		OutputEncoding: s3_model.NoEncoding,
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
		snapshotWriters:    make(map[S3ObjectRef]*SnapshotWriter),
		logger:             logger.Log,
		metrics:            stats.NewSinkerStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
		fileSplitter:       newFileSplitter(10, 0),
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

func newTestDropSink(t *testing.T, layout string, seed map[string][]byte) (*SnapshotSink, *testutil.MockS3Client) {
	t.Helper()
	mock := testutil.NewMockS3Client()
	bucket := "test-bucket"
	mock.BucketFiles[bucket] = seed
	cfg := &s3_model.S3Destination{
		Bucket:         bucket,
		OutputFormat:   model.ParsingFormatJSON,
		OutputEncoding: s3_model.NoEncoding,
		Layout:         layout,
	}
	return &SnapshotSink{
		cfg:             cfg,
		s3Client:        mock,
		logger:          logger.Log,
		metrics:         stats.NewSinkerStats(solomon.NewRegistry(nil)),
		snapshotWriters: make(map[S3ObjectRef]*SnapshotWriter),
		fileSplitter:    newFileSplitter(cfg.MaxItemsPerFile, cfg.MaxBytesPerFile),
	}, mock
}

func TestSnapshotSink_DropTable_StaticEmptyLayout(t *testing.T) {
	seed := map[string][]byte{
		"ns/tbl/part-1-abcd1234.00001.json":    []byte("x"),
		"ns/tbl/part-2-deadbeef.00002.json":    []byte("x"),
		"ns/tbl/part-3-cafebabe.00003.json.gz": []byte("x"),
		"ns/other/part-1-abcd1234.00001.json":  []byte("keep"),
	}
	sink, mock := newTestDropSink(t, "", seed)

	err := sink.dropTable(sink.makeS3ObjectRef(abstract.ChangeItem{
		Schema: "ns",
		Table:  "tbl",
	}))
	require.NoError(t, err)

	require.Len(t, mock.BucketFiles["test-bucket"], 1)
	require.Contains(t, mock.BucketFiles["test-bucket"], "ns/other/part-1-abcd1234.00001.json")
}

func TestSnapshotSink_DropTable_StaticNonEmptyLayout(t *testing.T) {
	seed := map[string][]byte{
		"fixed/ns/tbl/part-1-abcd1234.00001.json": []byte("x"),
		"fixed/ns/tbl/part-2-deadbeef.00002.json": []byte("x"),
		"other/ns/tbl/part-1-abcd1234.00001.json": []byte("keep"),
	}
	sink, mock := newTestDropSink(t, "fixed", seed)

	err := sink.dropTable(sink.makeS3ObjectRef(abstract.ChangeItem{
		Schema: "ns",
		Table:  "tbl",
	}))
	require.NoError(t, err)

	require.Len(t, mock.BucketFiles["test-bucket"], 1)
	require.Contains(t, mock.BucketFiles["test-bucket"], "other/ns/tbl/part-1-abcd1234.00001.json")
}

func TestSnapshotSink_DropTable_DynamicLayout(t *testing.T) {
	seed := map[string][]byte{
		"2026/07/01/ns/tbl/part-1-abcd1234.00001.json":    []byte("x"),
		"2026/07/02/ns/tbl/part-2-deadbeef.00002.json":    []byte("x"),
		"2026/07/02/ns/tbl/part-3-cafebabe.00003.json.gz": []byte("x"),
		"2026/07/02/ns/other/part-1-abcd1234.00001.json":  []byte("keep"),
		"random-file.txt": []byte("keep"),
	}
	sink, mock := newTestDropSink(t, "2006/01/02", seed)

	err := sink.dropTable(sink.makeS3ObjectRef(abstract.ChangeItem{
		Schema: "ns",
		Table:  "tbl",
	}))
	require.NoError(t, err)

	require.Len(t, mock.BucketFiles["test-bucket"], 2)
	require.Contains(t, mock.BucketFiles["test-bucket"], "2026/07/02/ns/other/part-1-abcd1234.00001.json")
	require.Contains(t, mock.BucketFiles["test-bucket"], "random-file.txt")
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

func TestSnapshotSink_Push_DropKindInvokesDropTable(t *testing.T) {
	seed := map[string][]byte{
		"2026/07/02/ns/tbl/part-1-abcd1234.00001.json":   []byte("x"),
		"2026/07/02/ns/tbl/part-2-deadbeef.00002.json":   []byte("x"),
		"2026/07/02/ns/other/part-1-abcd1234.00001.json": []byte("keep"),
	}
	sink, mock := newTestDropSink(t, "2006/01/02", seed)

	err := sink.Push([]abstract.ChangeItem{
		{
			Kind:   abstract.DropTableKind,
			Schema: "ns",
			Table:  "tbl",
		},
	})
	require.NoError(t, err)

	require.Len(t, mock.BucketFiles["test-bucket"], 1)
	require.Contains(t, mock.BucketFiles["test-bucket"], "2026/07/02/ns/other/part-1-abcd1234.00001.json")
	require.Len(t, mock.DeleteObjectsCalls, 1)
	require.Len(t, mock.DeleteObjectsCalls[0].Delete.Objects, 2)
}

func testSnapshotSinkDropPolicy(t *testing.T) {
	cfg := &s3_model.S3Destination{
		OutputFormat:   model.ParsingFormatJSON,
		OutputEncoding: s3_model.NoEncoding,
		Bucket:         "testSnapshotSinkDropPolicy",
		Cleanup:        model.Drop,
	}
	mockS3Client := testutil.NewMockS3Client()
	mockS3Client.BucketFiles[cfg.Bucket] = map[string][]byte{
		"test_table/part-1-abcd1234.00001.json": []byte("old snapshot data"),
		"other_table/should-remain.json":        []byte("keep me"),
	}

	testTimestamp := "1700000000"
	currSink := &SnapshotSink{
		s3Client:           mockS3Client,
		cfg:                cfg,
		operationTimestamp: testTimestamp,
		snapshotWriters:    make(map[S3ObjectRef]*SnapshotWriter),
		logger:             logger.Log,
		metrics:            stats.NewSinkerStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
		fileSplitter:       newFileSplitter(0, 0),
	}

	table := "test_table"
	require.NoError(t, currSink.Push([]abstract.ChangeItem{
		{Kind: abstract.InitTableLoad, CommitTime: uint64(time.Now().UnixNano()), Table: table},
	}))
	require.NoError(t, currSink.Push([]abstract.ChangeItem{
		{
			Kind:         abstract.InsertKind,
			CommitTime:   uint64(time.Now().UnixNano()),
			Table:        table,
			TableSchema:  testTableSchema,
			ColumnNames:  []string{"test1", "test2"},
			ColumnValues: []any{"value1", "value2"},
		},
	}))
	require.NoError(t, currSink.Push([]abstract.ChangeItem{
		{Kind: abstract.DoneTableLoad, CommitTime: uint64(time.Now().UnixNano()), Table: table},
	}))

	expectedKey := fmt.Sprintf("%s/part-%s-%s.00000.json", table, testTimestamp, hashPartID(""))
	require.Contains(t, mockS3Client.BucketFiles[cfg.Bucket], expectedKey)
	require.Contains(t, mockS3Client.BucketFiles[cfg.Bucket], "test_table/part-1-abcd1234.00001.json")
	require.Contains(t, mockS3Client.BucketFiles[cfg.Bucket], "other_table/should-remain.json")

	require.NoError(t, currSink.Push([]abstract.ChangeItem{
		{Kind: abstract.DropTableKind, CommitTime: uint64(time.Now().UnixNano()), Table: table},
	}))

	require.NotContains(t, mockS3Client.BucketFiles[cfg.Bucket], expectedKey)
	require.NotContains(t, mockS3Client.BucketFiles[cfg.Bucket], "test_table/part-1-abcd1234.00001.json")
	require.Contains(t, mockS3Client.BucketFiles[cfg.Bucket], "other_table/should-remain.json")
}
