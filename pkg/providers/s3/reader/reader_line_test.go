package reader

import (
	"bytes"
	"context"
	_ "embed"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/metrics/solomon"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers/s3"
	"github.com/transferria/transferria/pkg/stats"
)

var (
	fname = "data.log"
	//go:embed gotest/dump/data.log
	content []byte
)

func TestResolveLineSchema(t *testing.T) {
	src := s3.PrepareCfg(t, "barrel", model.ParsingFormatLine)

	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(src.ConnectionConfig.Endpoint),
		Region:           aws.String(src.ConnectionConfig.Region),
		S3ForcePathStyle: aws.Bool(src.ConnectionConfig.S3ForcePathStyle),
		Credentials: credentials.NewStaticCredentials(
			src.ConnectionConfig.AccessKey, string(src.ConnectionConfig.SecretKey), "",
		),
	})

	require.NoError(t, err)
	uploader := s3manager.NewUploader(sess)
	buff := bytes.NewReader(content)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Body:   buff,
		Bucket: aws.String(src.Bucket),
		Key:    aws.String(fname),
	})
	require.NoError(t, err)

	lineReader := LineReader{
		table:          abstract.TableID{},
		bucket:         src.Bucket,
		client:         aws_s3.New(sess),
		logger:         logger.Log,
		metrics:        stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
		tableSchema:    nil,
		batchSize:      1 * 1024 * 1024,
		blockSize:      1 * 1024 * 1024,
		pathPrefix:     "",
		pathPattern:    "",
		ColumnNames:    nil,
		hideSystemCols: false,
	}

	res, err := lineReader.ResolveSchema(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, res.Columns())

	t.Run("simple schema", func(t *testing.T) {
		schema, err := lineReader.ResolveSchema(context.Background())
		require.NoError(t, err)
		require.Len(t, schema.Columns(), 1)
		require.Equal(t, []string{"row"}, schema.Columns().ColumnNames())
		require.Equal(t, []string{"string"}, dataTypes(schema.Columns()))
	})
}
