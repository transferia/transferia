package snapshot

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/apache/arrow/go/v14/parquet/file"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	chrecipe "github.com/transferia/transferia/pkg/providers/clickhouse/recipe"
	s3_provider "github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	testBucket   = s3_provider.EnvOrDefault("TEST_BUCKET", "barrel")
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = *chrecipe.MustSource(chrecipe.WithInitFile("snapshot/dump/src.sql"), chrecipe.WithDatabase("clickhouse_test"))
)

func TestSnapshotParquet(t *testing.T) {
	s3Target := s3_provider.PrepareS3(t, testBucket, model.ParsingFormatPARQUET, s3_provider.GzipEncoding)
	s3Target.WithDefaults()

	require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "CH source", Port: Source.NativePort},
	))
	Source.WithDefaults()

	helpers.InitSrcDst(helpers.TransferID, &Source, s3Target, abstract.TransferTypeSnapshotOnly)
	// checking the bucket is empty
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(s3Target.Endpoint),
		Region:           aws.String(s3Target.Region),
		S3ForcePathStyle: aws.Bool(s3Target.S3ForcePathStyle),
		Credentials: credentials.NewStaticCredentials(
			s3Target.AccessKey, s3Target.Secret, "",
		),
	})
	require.NoError(t, err)

	objects, err := s3.New(sess).ListObjects(&s3.ListObjectsInput{Bucket: &s3Target.Bucket})
	require.NoError(t, err)

	logger.Log.Infof("objects: %v", objects.String())
	require.Len(t, objects.Contents, 0)

	time.Sleep(5 * time.Second)

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, s3Target, TransferType)
	helpers.Activate(t, transfer)

	sess, err = session.NewSession(&aws.Config{
		Endpoint:         aws.String(s3Target.Endpoint),
		Region:           aws.String(s3Target.Region),
		S3ForcePathStyle: aws.Bool(s3Target.S3ForcePathStyle),
		Credentials: credentials.NewStaticCredentials(
			s3Target.AccessKey, s3Target.Secret, "",
		),
	})
	require.NoError(t, err)

	objects, err = s3.New(sess).ListObjects(&s3.ListObjectsInput{Bucket: &s3Target.Bucket})
	require.NoError(t, err)
	logger.Log.Infof("objects: %v", objects.String())

	require.Len(t, objects.Contents, 1)
	obj, err := s3.New(sess).GetObject(&s3.GetObjectInput{Bucket: &s3Target.Bucket, Key: objects.Contents[0].Key})
	require.NoError(t, err)

	parquetRowCount, err := getParquetFileRowCount(obj)
	require.NoError(t, err)

	tableRowCount, err := getClickHouseTableRowCount()

	require.NoError(t, err)
	require.Equal(t, tableRowCount, parquetRowCount, "No of rows in table does not match parquet file row count")
}

func getClickHouseTableRowCount() (int64, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("localhost:%d", Source.NativePort)},
		Auth: clickhouse.Auth{
			Database: Source.Database,
			Username: Source.User,
		},
		Protocol: clickhouse.Native, // Native protocol (faster)
	})
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	var rowCount uint64
	err = conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM clickhouse_test.sample").Scan(&rowCount)
	if err != nil {
		return 0, err
	}

	return int64(rowCount), nil
}

func getParquetFileRowCount(obj *s3.GetObjectOutput) (int64, error) {
	gzippedData, err := io.ReadAll(obj.Body)
	if err != nil {
		return 0, err
	}
	defer obj.Body.Close()

	reader, err := gzip.NewReader(bytes.NewReader(gzippedData))
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	parquetData, err := io.ReadAll(reader)
	if err != nil {
		return 0, err
	}

	parquetReader, err := file.NewParquetReader(bytes.NewReader(parquetData))
	if err != nil {
		return 0, err
	}
	defer parquetReader.Close()

	rowCount := parquetReader.MetaData().NumRows
	return rowCount, nil
}
