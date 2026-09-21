package snapshot

import (
	"fmt"
	"io"
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
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/s3"
	_ "github.com/transferia/transferia/tests/helpers/registration/ydb"
	"github.com/transferia/transferia/tests/helpers/testenv"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	"go.ytsaurus.tech/library/go/core/log"
	ytschema "go.ytsaurus.tech/yt/go/schema"
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

func createBucket(t *testing.T, cfg *s3_model.S3Destination) {
	sess, err := aws_session.NewSession(&aws.Config{
		Endpoint:         aws.String(cfg.Endpoint),
		Region:           aws.String(cfg.Region),
		S3ForcePathStyle: aws.Bool(cfg.S3ForcePathStyle),
		Credentials: aws_credentials.NewStaticCredentials(
			cfg.AccessKey, cfg.Secret, "",
		),
	})
	require.NoError(t, err)
	logger.Log.Info("create bucket", log.Any("bucket", cfg.Bucket))
	res, err := aws_s3.New(sess).CreateBucket(&aws_s3.CreateBucketInput{
		Bucket: aws.String(cfg.Bucket),
	})
	require.NoError(t, err)
	logger.Log.Info("create bucket result", log.Any("res", res))
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestGroup(t *testing.T) {
	src := &provider_ydb.YdbSource{
		Token:              model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:           testenv.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:           testenv.GetEnvOfFail(t, "YDB_ENDPOINT"),
		Tables:             nil,
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
	}
	dst := &s3_model.S3Destination{
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

	sourcePort, err := network.GetPortFromStr(src.Instance)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "YDB source", Port: sourcePort},
		))
	}()

	transferhelpers.InitSrcDst(transferhelpers.TransferID, src, dst, abstract.TransferTypeSnapshotOnly)

	// init data
	Target := &provider_ydb.YdbDestination{
		Database: src.Database,
		Token:    src.Token,
		Instance: src.Instance,
	}
	Target.WithDefaults()
	sinker, err := provider_ydb.NewSinker(logger.Log, Target, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)
	testSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "id", DataType: string(ytschema.TypeInt32), PrimaryKey: true},
		{ColumnName: "val", DataType: string(ytschema.TypeAny), OriginalType: "ydb:Yson"},
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
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, src, dst, abstract.TransferTypeSnapshotOnly)
	delivery.Activate(t, transfer)

	// check data
	sess, err := aws_session.NewSession(&aws.Config{
		Endpoint:         aws.String(dst.Endpoint),
		Region:           aws.String(dst.Region),
		S3ForcePathStyle: aws.Bool(dst.S3ForcePathStyle),
		Credentials: aws_credentials.NewStaticCredentials(
			dst.AccessKey, dst.Secret, "",
		),
	})

	require.NoError(t, err)
	s3client := aws_s3.New(sess)
	objects, err := s3client.ListObjects(&aws_s3.ListObjectsInput{
		Bucket: aws.String(dst.Bucket),
	})
	require.NoError(t, err)
	logger.Log.Infof("objects: %v", objects.Contents)
	require.Len(t, objects.Contents, 1)
	obj, err := s3client.GetObject(&aws_s3.GetObjectInput{Bucket: aws.String(dst.Bucket), Key: objects.Contents[0].Key})
	require.NoError(t, err)
	data, err := io.ReadAll(obj.Body)
	require.NoError(t, err)
	logger.Log.Infof("read file: %s /n%s", *objects.Contents[0].Key, string(data))
	expectedPrefix := "test/foo/insert_into_s3/part-"
	comment := fmt.Sprintf("expected prefix: %s, got: %s", expectedPrefix, *objects.Contents[0].Key)
	require.True(t, strings.HasPrefix(*objects.Contents[0].Key, expectedPrefix), comment)
}
