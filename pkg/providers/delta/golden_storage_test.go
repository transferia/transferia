//go:build !disable_delta_provider

package delta

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
)

// badGoldenTest it's a set of cases that doomed to fail
// that was designed that way
var badGoldenTest = set.New(
	// todo: special character, s3 client fail them
	"deltatbl-special-chars-in-partition-column",
	"data-reader-escaped-chars",
	"data-reader-partition-values",
)

var (
	testBucket    = envOrDefault("TEST_BUCKET", "barrel")
	testAccessKey = envOrDefault("TEST_ACCESS_KEY_ID", "1234567890")
	testSecret    = envOrDefault("TEST_SECRET_ACCESS_KEY", "abcdefabcdef")
)

func envOrDefault(key string, def string) string {
	if os.Getenv(key) != "" {
		return os.Getenv(key)
	}
	return def
}

func TestSnapshotData3(t *testing.T) {
	testCasePath := "golden/snapshot-data3"
	cfg := prepareCfg(t)
	cfg.PathPrefix = testCasePath
	if os.Getenv("S3MDS_PORT") != "" { // for local recipe we need to upload test case to internet
		cfg.Bucket = "data3"
		createBucket(t, cfg)
		prepareTestCase(t, cfg, testCasePath)
	} else {
		cfg.PathPrefix = os.Getenv("S3_PREFIX") + cfg.PathPrefix
	}
	logger.Log.Info("dir uploaded")
	storage, err := NewStorage(cfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)
	schema, err := storage.TableList(nil)
	require.NoError(t, err)
	tid := *abstract.NewTableID("test_namespace", "test_name")
	for _, col := range schema[tid].Schema.Columns() {
		logger.Log.Infof("resolved schema: %s (%s) %v", col.ColumnName, col.DataType, col.PrimaryKey)
	}
	totalRows, err := storage.ExactTableRowsCount(tid)
	require.NoError(t, err)
	logger.Log.Infof("estimate %v rows", totalRows)
	require.Equal(t, 30, int(totalRows))
	tdesc, err := storage.ShardTable(context.Background(), abstract.TableDescription{Name: tid.Name, Schema: tid.Namespace})
	require.NoError(t, err)
	require.Len(t, tdesc, 4)
	for _, desc := range tdesc {
		require.NoError(
			t,
			storage.LoadTable(context.Background(), desc, func(items []abstract.ChangeItem) error {
				abstract.Dump(items)
				return nil
			}),
		)
	}
}

func prepareTestCase(t *testing.T, cfg *DeltaSource, casePath string) {
	absPath, err := filepath.Abs(casePath)
	require.NoError(t, err)
	files, err := os.ReadDir(absPath)
	require.NoError(t, err)
	logger.Log.Info("dir read done")
	uploadDir(t, cfg, cfg.PathPrefix, files)
}

func uploadDir(t *testing.T, cfg *DeltaSource, prefix string, files []os.DirEntry) {
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(cfg.Endpoint),
		Region:           aws.String(cfg.Region),
		S3ForcePathStyle: aws.Bool(cfg.S3ForcePathStyle),
		Credentials: credentials.NewStaticCredentials(
			cfg.AccessKey, string(cfg.SecretKey), "",
		),
	})
	require.NoError(t, err)
	uploader := s3manager.NewUploader(sess)
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
		uploadOne(t, cfg, fullName, uploader)
	}
}

func uploadOne(t *testing.T, cfg *DeltaSource, fname string, uploader *s3manager.Uploader) {
	absPath, err := filepath.Abs(fname)
	require.NoError(t, err)
	buff, err := os.Open(absPath)
	require.NoError(t, err)
	defer buff.Close()
	_, err = uploader.Upload(&s3manager.UploadInput{
		Body:   buff,
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(fname),
	})
	require.NoError(t, err)
}

// this only works for local use
func TestGoldenDataSet(t *testing.T) {
	if os.Getenv("S3MDS_PORT") != "" {
		t.Skip() // only works with premade s3 bucket for now
	}

	golden, err := os.ReadDir("golden")
	require.NoError(t, err)

	cfg := prepareCfg(t)

	for _, entry := range golden {
		if badGoldenTest.Contains(entry.Name()) {
			continue
		}
		t.Run(entry.Name(), func(t *testing.T) {
			path, err := filepath.Abs("golden/" + entry.Name())
			require.NoError(t, err)
			readCase(t, path, cfg)
		})
	}
}

func prepareCfg(t *testing.T) *DeltaSource {
	cfg := &DeltaSource{
		Bucket:           testBucket,
		AccessKey:        testAccessKey,
		S3ForcePathStyle: true,
		SecretKey:        model.SecretString(testSecret),
		TableNamespace:   "test_namespace",
		TableName:        "test_name",
	}

	if os.Getenv("S3MDS_PORT") != "" {
		cfg.Endpoint = fmt.Sprintf("http://localhost:%v", os.Getenv("S3MDS_PORT"))
		cfg.Bucket = "delta-sample"
		cfg.Region = "ru-central1"
		createBucket(t, cfg)
	} else if os.Getenv("S3_ACCESS_KEY") != "" {
		// to go to real S3
		cfg.Endpoint = os.Getenv("S3_ENDPOINT")
		cfg.AccessKey = os.Getenv("S3_ACCESS_KEY")
		cfg.SecretKey = model.SecretString(os.Getenv("S3_SECRET"))
		cfg.Bucket = os.Getenv("S3_BUCKET")
		cfg.Region = os.Getenv("S3_REGION")
	}
	return cfg
}

func readCase(t *testing.T, path string, cfg *DeltaSource) {
	subF, err := os.ReadDir(path)
	require.NoError(t, err)
	isDelatLog := false
	for _, f := range subF {
		if f.IsDir() && f.Name() == "_delta_log" {
			isDelatLog = true
		}
	}
	if !isDelatLog {
		for _, f := range subF {
			if f.IsDir() {
				if badGoldenTest.Contains(f.Name()) {
					continue
				}
				t.Run(f.Name(), func(t *testing.T) {
					readCase(t, path+"/"+f.Name(), cfg)
				})
			}
		}
		return
	}
	if len(subF) == 1 {
		// delta-log folder with just log, ignore
		return
	}

	goldenPath, err := filepath.Abs("golden")
	require.NoError(t, err)
	clearedPath := strings.ReplaceAll(path, goldenPath, os.Getenv("S3_PREFIX")+"golden")
	cfg.PathPrefix = clearedPath

	storage, err := NewStorage(cfg, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)
	schema, err := storage.TableList(nil)
	require.NoError(t, err)
	tid := *abstract.NewTableID("test_namespace", "test_name")
	for _, col := range schema[tid].Schema.Columns() {
		logger.Log.Infof("resolved schema: %s (%s) %v", col.ColumnName, col.DataType, col.PrimaryKey)
	}
	totalRows, err := storage.ExactTableRowsCount(tid)
	require.NoError(t, err)
	logger.Log.Infof("estimate %v rows", totalRows)
}

func createBucket(t *testing.T, cfg *DeltaSource) {
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(cfg.Endpoint),
		Region:           aws.String(cfg.Region),
		S3ForcePathStyle: aws.Bool(cfg.S3ForcePathStyle),
		Credentials: credentials.NewStaticCredentials(
			cfg.AccessKey, string(cfg.SecretKey), "",
		),
	})
	require.NoError(t, err)
	res, err := s3.New(sess).CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(cfg.Bucket),
	})
	require.NoError(t, err)
	logger.Log.Info("create bucket result", log.Any("res", res))
}
