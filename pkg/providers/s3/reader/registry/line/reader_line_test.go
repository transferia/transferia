package line

import (
	"bytes"
	"context"
	_ "embed"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/providers/s3/s3recipe"
	"github.com/transferia/transferia/pkg/stats"
)

var (
	fileName = "data.log"
	//go:embed gotest/dump/data.log
	content []byte
)

func TestResolveLineSchema(t *testing.T) {
	src := s3recipe.PrepareCfg(t, "barrel", model.ParsingFormatLine)

	sess, err := session.NewSession(
		&aws.Config{
			Endpoint:         aws.String(src.ConnectionConfig.Endpoint),
			Region:           aws.String(src.ConnectionConfig.Region),
			S3ForcePathStyle: aws.Bool(src.ConnectionConfig.S3ForcePathStyle),
			Credentials: credentials.NewStaticCredentials(
				src.ConnectionConfig.AccessKey,
				string(src.ConnectionConfig.SecretKey),
				"",
			),
		},
	)

	require.NoError(t, err)
	uploader := s3manager.NewUploader(sess)
	buff := bytes.NewReader(content)
	_, err = uploader.Upload(
		&s3manager.UploadInput{
			Body:   buff,
			Bucket: aws.String(src.Bucket),
			Key:    aws.String(fileName),
		},
	)
	require.NoError(t, err)

	resolver, err := NewLineSchemaResolver(src, logger.Log, sess, stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())), s3raw.NewRealS3RawReaderBuilder())
	require.NoError(t, err)

	res, err := reader.ExecuteSchemaResolver(context.Background(), resolver)
	require.NoError(t, err)
	require.NotEmpty(t, res.Columns())

	t.Run("simple schema", func(t *testing.T) {
		schema, err := reader.ExecuteSchemaResolver(context.Background(), resolver)
		require.NoError(t, err)
		require.Len(t, schema.Columns(), 1)
		require.Equal(t, []string{"row"}, schema.Columns().ColumnNames())
		require.Equal(t, []string{"string"}, reader.DataTypes(schema.Columns()))
	})
}
