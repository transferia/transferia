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
	"go.ytsaurus.tech/yt/go/schema"
)

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

//
// Tests
//

func TestS3SinkUploadTable(t *testing.T) {
	cfg := s3recipe.PrepareS3(t, "TestS3SinkUploadTable", model.ParsingFormatCSV, s3_provider.GzipEncoding)
	cfg.Layout = "e2e_test-2006-01-02"
	cp := testutil.NewFakeClientWithTransferState()
	currSink, err := NewSnapshotSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, "TestS3SinkUploadTable")
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
	cfg := s3recipe.PrepareS3(t, "TestS3SinkBucketTZ", model.ParsingFormatCSV, s3_provider.GzipEncoding)
	cfg.Layout = "02 Jan 06 15:04 MST"
	cfg.LayoutTZ = "CET"

	cp := testutil.NewFakeClientWithTransferState()
	currSink, err := NewSnapshotSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, "TestS3SinkBucketTZ")
	require.NoError(t, err)
	b := currSink.bucket(abstract.ChangeItem{Kind: abstract.DoneTableLoad, CommitTime: uint64(time.Date(2022, time.Month(10), 19, 0, 0, 0, 0, time.UTC).UnixNano()), Table: "test_table"})
	require.Equal(t, "19 Oct 22 02:00 CEST", b)

	cfg.LayoutTZ = "UTC"
	b = currSink.bucket(abstract.ChangeItem{Kind: abstract.DoneTableLoad, CommitTime: uint64(time.Date(2022, time.Month(10), 19, 0, 0, 0, 0, time.UTC).UnixNano()), Table: "test_table"})
	require.Equal(t, "19 Oct 22 00:00 UTC", b)
}

func TestS3SinkUploadTableGzip(t *testing.T) {
	cfg := s3recipe.PrepareS3(t, "TestS3SinkUploadTableGzip", model.ParsingFormatCSV, s3_provider.GzipEncoding)
	cfg.Layout = "test_gzip"

	cp := testutil.NewFakeClientWithTransferState()
	currSink, err := NewSnapshotSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, "TestS3SinkUploadTableGzip")
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

func TestJsonSnapshot(t *testing.T) {
	bucket := "testjsonnoencode"
	cfg := s3recipe.PrepareS3(t, bucket, model.ParsingFormatJSON, s3_provider.NoEncoding)
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

			currSink, err := NewSnapshotSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()), cp, "TestJSONSnapshot")
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
