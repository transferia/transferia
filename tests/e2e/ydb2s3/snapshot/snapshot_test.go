package snapshot

import (
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_provider "github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/helpers"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
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

func createBucket(t *testing.T, cfg *s3_provider.S3Destination) {
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(cfg.Endpoint),
		Region:           aws.String(cfg.Region),
		S3ForcePathStyle: aws.Bool(cfg.S3ForcePathStyle),
		Credentials: credentials.NewStaticCredentials(
			cfg.AccessKey, cfg.Secret, "",
		),
	})
	require.NoError(t, err)
	logger.Log.Info("create bucket", log.Any("bucket", cfg.Bucket))
	res, err := s3.New(sess).CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(cfg.Bucket),
	})
	require.NoError(t, err)
	logger.Log.Info("create bucket result", log.Any("res", res))
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestGroup(t *testing.T) {
	src := &ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           helpers.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           helpers.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             nil,
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
	}
	dst := &s3_provider.S3Destination{
		OutputFormat:     model.ParsingFormatJSON,
		BufferSize:       1 * 1024 * 1024,
		BufferInterval:   time.Second * 5,
		Bucket:           testBucket,
		AccessKey:        testAccessKey,
		S3ForcePathStyle: true,
		Secret:           testSecret,
		Layout:           "test",
		Region:           "eu-central1",
	}
	dst.WithDefaults()

	if os.Getenv("S3MDS_PORT") != "" {
		dst.Endpoint = fmt.Sprintf("http://localhost:%v", os.Getenv("S3MDS_PORT"))
		createBucket(t, dst)
	}

	sourcePort, err := helpers.GetPortFromStr(src.Instance)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "YDB source", Port: sourcePort},
		))
	}()

	helpers.InitSrcDst(helpers.TransferID, src, dst, abstract.TransferTypeSnapshotOnly)

	// init data
	Target := &ydb.YdbDestination{
		Database: src.Database,
		Token:    src.Token,
		Instance: src.Instance,
	}
	Target.WithDefaults()
	sinker, err := ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)
	testSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "id", DataType: string(schema.TypeInt32), PrimaryKey: true},
		{ColumnName: "val", DataType: string(schema.TypeAny), OriginalType: "ydb:Yson"},
	})
	require.NoError(t, sinker.Push([]abstract.ChangeItem{{
		Kind:         abstract.InsertKind,
		Schema:       "",
		Table:        "foo/insert_into_s3",
		ColumnNames:  []string{"id", "val"},
		ColumnValues: []interface{}{1, map[string]interface{}{"a": 123}},
		TableSchema:  testSchema,
	}}))

	// activate transfer
	transfer := helpers.MakeTransfer(helpers.TransferID, src, dst, abstract.TransferTypeSnapshotOnly)
	helpers.Activate(t, transfer)

	// check data
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(dst.Endpoint),
		Region:           aws.String(dst.Region),
		S3ForcePathStyle: aws.Bool(dst.S3ForcePathStyle),
		Credentials: credentials.NewStaticCredentials(
			dst.AccessKey, dst.Secret, "",
		),
	})

	require.NoError(t, err)
	s3client := s3.New(sess)
	objects, err := s3client.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(dst.Bucket),
	})
	require.NoError(t, err)
	logger.Log.Infof("objects: %v", objects.Contents)
	require.Len(t, objects.Contents, 1)
	obj, err := s3client.GetObject(&s3.GetObjectInput{Bucket: aws.String(dst.Bucket), Key: objects.Contents[0].Key})
	require.NoError(t, err)
	data, err := io.ReadAll(obj.Body)
	require.NoError(t, err)
	logger.Log.Infof("read file: %s /n%s", *objects.Contents[0].Key, string(data))
	expectedPrefix := "test/foo/insert_into_s3/part-"
	comment := fmt.Sprintf("expected prefix: %s, got: %s", expectedPrefix, *objects.Contents[0].Key)
	require.True(t, strings.HasPrefix(*objects.Contents[0].Key, expectedPrefix), comment)
}
