package snapshot

import (
	"fmt"
	"io"
	"os"
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
	"github.com/transferia/transferia/pkg/providers/postgres"
	s3_provider "github.com/transferia/transferia/pkg/providers/s3"
	_ "github.com/transferia/transferia/pkg/providers/s3/provider"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	"go.ytsaurus.tech/library/go/core/log"
)

var (
	testBucket    = envOrDefault("TEST_BUCKET", "barrel")
	testAccessKey = envOrDefault("TEST_ACCESS_KEY_ID", "1234567890")
	testSecret    = envOrDefault("TEST_SECRET_ACCESS_KEY", "abcdefabcdef")
)

var (
	Source = postgres.PgSource{
		ClusterID: os.Getenv("PG_CLUSTER_ID"),
		Hosts:     []string{"localhost"},
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      helpers.GetIntFromEnv("PG_LOCAL_PORT"),
		DBTables:  []string{"public.__test"},
	}
	Target = &s3_provider.S3Destination{
		OutputFormat:   model.ParsingFormatJSON,
		BufferSize:     1 * 1024 * 1024,
		BufferInterval: time.Second * 5,
		Bucket:         testBucket,
		AccessKey:      testAccessKey,
		Secret:         testSecret,
		Region:         "eu-central1",
		Layout:         "e2e_test-2006-01-02",
		AnyAsString:    true,
		LayoutColumn:   "ts",
	}
)

func envOrDefault(key string, def string) string {
	if os.Getenv(key) != "" {
		return os.Getenv(key)
	}
	return def
}

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

func createBucket(t *testing.T, cfg *s3_provider.S3Destination) {
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(cfg.Endpoint),
		Region:           aws.String(cfg.Region),
		S3ForcePathStyle: aws.Bool(true),
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

func checkBucket(t *testing.T, cfg *s3_provider.S3Destination, size int) {
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(cfg.Endpoint),
		Region:           aws.String(cfg.Region),
		S3ForcePathStyle: aws.Bool(true),
		Credentials: credentials.NewStaticCredentials(
			cfg.AccessKey, cfg.Secret, "",
		),
	})
	require.NoError(t, err)
	objs, err := s3.New(sess).ListObjects(&s3.ListObjectsInput{Bucket: &cfg.Bucket})
	require.NoError(t, err)
	logger.Log.Infof("objects: %v", objs.String())
	require.Len(t, objs.Contents, size)
	for _, content := range objs.Contents {
		obj, err := s3.New(sess).GetObject(&s3.GetObjectInput{Bucket: &cfg.Bucket, Key: content.Key})
		require.NoError(t, err)
		data, err := io.ReadAll(obj.Body)
		require.NoError(t, err)
		logger.Log.Infof("object: %v content:\n%v", *content.Key, string(data))
	}
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
		))
	}()

	Target.WithDefaults()

	if os.Getenv("S3MDS_PORT") != "" {
		Target.Endpoint = fmt.Sprintf("http://localhost:%v", os.Getenv("S3MDS_PORT"))
		Target.Bucket = "TestS3SinkerUploadTable"
		createBucket(t, Target)
	}

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Existence", Existence)
		t.Run("Verify", Verify)
		t.Run("Snapshot", Snapshot)
	})
}

func Existence(t *testing.T) {
	_, err := postgres.NewStorage(Source.ToStorageParams(nil))
	require.NoError(t, err)
}

func Verify(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotOnly)
	err := tasks.VerifyDelivery(*transfer, logger.Log, helpers.EmptyRegistry())
	require.Error(t, err, "sink: no InitTableLoad event")
	checkBucket(t, Target, 0)
}

func Snapshot(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotOnly)
	helpers.Activate(t, transfer)
	checkBucket(t, Target, 2)
}
