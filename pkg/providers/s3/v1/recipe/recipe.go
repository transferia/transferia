package s3recipe

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	aws_credentials "github.com/aws/aws-sdk-go/aws/credentials"
	aws_session "github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
	"github.com/transferia/transferia/tests/tcrecipes"
	"github.com/transferia/transferia/tests/tcrecipes/objectstorage"
	"go.ytsaurus.tech/library/go/core/log"
)

var (
	testBucket    = EnvOrDefault("TEST_BUCKET", "barrel")
	testAccessKey = EnvOrDefault("TEST_ACCESS_KEY_ID", "1234567890")
	testSecret    = EnvOrDefault("TEST_SECRET_ACCESS_KEY", "abcdefabcdef")
)

func createBucket(t *testing.T, cfg *s3_v1_model.S3Destination) {
	sess, err := aws_session.NewSession(&aws.Config{
		Endpoint:         aws.String(cfg.Connection.Endpoint),
		Region:           aws.String(cfg.Connection.Region),
		S3ForcePathStyle: aws.Bool(cfg.Connection.S3ForcePathStyle),
		Credentials: aws_credentials.NewStaticCredentials(
			cfg.Connection.AccessKey, string(cfg.Connection.SecretKey), "",
		),
	})
	require.NoError(t, err)
	res, err := aws_s3.New(sess).CreateBucket(&aws_s3.CreateBucketInput{
		Bucket: aws.String(cfg.Bucket),
	})
	require.NoError(t, err)

	logger.Log.Info("create bucket result", log.Any("res", res))
}

func PrepareS3(t *testing.T, bucket string, serializer s3_v1_model.SerializerConfig) *s3_v1_model.S3Destination {
	if tcrecipes.Enabled() {
		_, err := objectstorage.Prepare(context.Background())
		require.NoError(t, err)
	}

	connection := s3_model.ConnectionConfig{
		AccessKey:        testAccessKey,
		S3ForcePathStyle: true,
		SecretKey:        model.SecretString(testSecret),
		Endpoint:         "",
		UseSSL:           false,
		VerifySSL:        false,
		Region:           "",
		ServiceAccountID: "",
	}

	if os.Getenv("S3_ACCESS_KEY") != "" {
		connection.Endpoint = os.Getenv("S3_ENDPOINT")
		connection.AccessKey = os.Getenv("S3_ACCESS_KEY")
		connection.SecretKey = model.SecretString(os.Getenv("S3_SECRET"))
		connection.Region = os.Getenv("S3_REGION")
	} else {
		connection.Endpoint = fmt.Sprintf("http://localhost:%v", os.Getenv("S3MDS_PORT"))
		connection.Region = "ru-central1"
	}

	serializerUnion := s3_v1_model.SerializerUnion{
		Json:    nil,
		CSV:     nil,
		Parquet: nil,
	}
	switch v := serializer.(type) {
	case *s3_v1_model.JsonSerializerConfig:
		serializerUnion.Json = v
	case *s3_v1_model.CSVSerializerConfig:
		serializerUnion.CSV = v
	case *s3_v1_model.ParquetSerializerConfig:
		serializerUnion.Parquet = v
	}

	cfg := &s3_v1_model.S3Destination{
		BufferSize:        1 * 1024 * 1024,
		BufferInterval:    time.Second * 5,
		Connection:        connection,
		SerializerType:    serializer.FormatName(),
		Serializer:        serializerUnion,
		Bucket:            strings.ToLower(bucket),
		PartSize:          0,
		Prefix:            "",
		MaxItemsPerFile:   0,
		MaxBytesPerFile:   0,
		SerializerSet:     false,
		RotatorType:       s3_v1_model.DefaultRotator,
		RotatorConfig:     s3_v1_model.RotatorUnion{Default: &s3_v1_model.DefaultRotatorConfig{Interval: time.Hour}},
		PartitionerType:   s3_v1_model.DefaultPartitioner,
		PartitionerConfig: s3_v1_model.PartitionerUnion{Default: &s3_v1_model.DefaultPartitionerConfig{}},
	}
	cfg.WithDefaults()

	createBucket(t, cfg)
	return cfg
}

func EnvOrDefault(key string, def string) string {
	if os.Getenv(key) != "" {
		return os.Getenv(key)
	}
	return def
}
