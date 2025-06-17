//go:build !disable_s3_provider

package s3

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/tests/tcrecipes"
	"github.com/transferia/transferia/tests/tcrecipes/objectstorage"
	"go.ytsaurus.tech/library/go/core/log"
)

var (
	testBucket    = EnvOrDefault("TEST_BUCKET", "barrel")
	testAccessKey = EnvOrDefault("TEST_ACCESS_KEY_ID", "1234567890")
	testSecret    = EnvOrDefault("TEST_SECRET_ACCESS_KEY", "abcdefabcdef")
)

func createBucket(t *testing.T, cfg *S3Destination) {
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(cfg.Endpoint),
		Region:           aws.String(cfg.Region),
		S3ForcePathStyle: aws.Bool(cfg.S3ForcePathStyle),
		Credentials: credentials.NewStaticCredentials(
			cfg.AccessKey, cfg.Secret, "",
		),
	})
	require.NoError(t, err)
	res, err := s3.New(sess).CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(cfg.Bucket),
	})
	require.NoError(t, err)

	logger.Log.Info("create bucket result", log.Any("res", res))
}

func PrepareS3(t *testing.T, bucket string, format model.ParsingFormat, encoding Encoding) *S3Destination {
	if tcrecipes.Enabled() {
		_, err := objectstorage.Prepare(context.Background())
		require.NoError(t, err)
	}
	cfg := &S3Destination{
		OutputFormat:     format,
		OutputEncoding:   encoding,
		BufferSize:       1 * 1024 * 1024,
		BufferInterval:   time.Second * 5,
		Endpoint:         "",
		Region:           "",
		AccessKey:        testAccessKey,
		S3ForcePathStyle: true,
		Secret:           testSecret,
		ServiceAccountID: "",
		Layout:           "",
		LayoutTZ:         "",
		LayoutColumn:     "",
		Bucket:           testBucket,
		UseSSL:           false,
		VerifySSL:        false,
		PartSize:         0,
		Concurrency:      0,
		AnyAsString:      false,
	}
	cfg.WithDefaults()
	bucket = strings.ToLower(bucket)
	if os.Getenv("S3_ACCESS_KEY") != "" {
		cfg.Endpoint = os.Getenv("S3_ENDPOINT")
		cfg.AccessKey = os.Getenv("S3_ACCESS_KEY")
		cfg.Secret = os.Getenv("S3_SECRET")
		cfg.Bucket = bucket
		cfg.Region = os.Getenv("S3_REGION")
	} else {
		cfg.Endpoint = fmt.Sprintf("http://localhost:%v", os.Getenv("S3MDS_PORT"))
		cfg.Bucket = bucket
		cfg.Region = "ru-central1"
	}
	createBucket(t, cfg)
	return cfg
}

func EnvOrDefault(key string, def string) string {
	if os.Getenv(key) != "" {
		return os.Getenv(key)
	}
	return def
}

func PrepareCfg(t *testing.T, bucket string, format model.ParsingFormat) *S3Source {
	if tcrecipes.Enabled() {
		_, err := objectstorage.Prepare(context.Background())
		require.NoError(t, err)
	}
	cfg := new(S3Source)
	if bucket != "" {
		cfg.Bucket = bucket
	} else {
		cfg.Bucket = testBucket
	}

	if format != "" {
		cfg.InputFormat = format
	} else {
		cfg.InputFormat = model.ParsingFormatPARQUET
	}
	cfg.ConnectionConfig.AccessKey = testAccessKey
	cfg.ConnectionConfig.S3ForcePathStyle = true
	cfg.ConnectionConfig.SecretKey = model.SecretString(testSecret)
	cfg.ConnectionConfig.Region = "ru-central1"
	cfg.ConnectionConfig.Endpoint = fmt.Sprintf("http://localhost:%v", os.Getenv("S3MDS_PORT"))

	cfg.TableNamespace = "test_namespace"
	cfg.TableName = "test_name"
	if os.Getenv("S3_ACCESS_KEY") != "" {
		// to go to real S3
		if os.Getenv("S3_BUCKET") != "" {
			cfg.Bucket = os.Getenv("S3_BUCKET")
		}
		cfg.ConnectionConfig.Endpoint = os.Getenv("S3_ENDPOINT")
		cfg.ConnectionConfig.AccessKey = os.Getenv("S3_ACCESS_KEY")
		cfg.ConnectionConfig.SecretKey = model.SecretString(os.Getenv("S3_SECRET"))
		cfg.ConnectionConfig.Region = os.Getenv("S3_REGION")
	}
	if os.Getenv("S3MDS_PORT") != "" {
		CreateBucket(t, cfg)
	}
	return cfg
}

func PrepareTestCase(t *testing.T, cfg *S3Source, casePath string) {
	absPath, err := filepath.Abs(casePath)
	require.NoError(t, err)
	files, err := os.ReadDir(absPath)
	require.NoError(t, err)
	logger.Log.Info("dir read done")
	uploadDir(t, cfg, cfg.PathPrefix, files)
}

func uploadDir(t *testing.T, cfg *S3Source, prefix string, files []os.DirEntry) {
	for _, file := range files {
		fullName := fmt.Sprintf("%s/%s", prefix, file.Name())
		if file.IsDir() {
			absPath, err := filepath.Abs(fullName)
			require.NoError(t, err)
			dirFiles, err := os.ReadDir(absPath)
			require.NoError(t, err)
			uploadDir(t, cfg, fullName, dirFiles)
			continue
		}
		UploadOne(t, cfg, fullName)
	}
}

func UploadOne(t *testing.T, cfg *S3Source, fname string) {
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(cfg.ConnectionConfig.Endpoint),
		Region:           aws.String(cfg.ConnectionConfig.Region),
		S3ForcePathStyle: aws.Bool(cfg.ConnectionConfig.S3ForcePathStyle),
		Credentials: credentials.NewStaticCredentials(
			cfg.ConnectionConfig.AccessKey, string(cfg.ConnectionConfig.SecretKey), "",
		),
	})
	require.NoError(t, err)
	uploader := s3manager.NewUploader(sess)
	absPath, err := filepath.Abs(fname)
	require.NoError(t, err)
	buff, err := os.Open(absPath)
	require.NoError(t, err)
	defer buff.Close()
	logger.Log.Infof("will upload to bucket %s", cfg.Bucket)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Body:   buff,
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(fname),
	})
	require.NoError(t, err)
}

func CreateBucket(t *testing.T, cfg *S3Source) {
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(cfg.ConnectionConfig.Endpoint),
		Region:           aws.String(cfg.ConnectionConfig.Region),
		S3ForcePathStyle: aws.Bool(cfg.ConnectionConfig.S3ForcePathStyle),
		Credentials: credentials.NewStaticCredentials(
			cfg.ConnectionConfig.AccessKey, string(cfg.ConnectionConfig.SecretKey), "",
		),
	})
	require.NoError(t, err)
	res, err := s3.New(sess).CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(cfg.Bucket),
	})
	// No need to check error because maybe the bucket can be already exists
	logger.Log.Info("create bucket result", log.Any("res", res), log.Error(err))
}
