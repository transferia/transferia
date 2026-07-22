package cleanupreplace

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_storage "github.com/transferia/transferia/pkg/providers/s3/storage"
	_ "github.com/transferia/transferia/pkg/providers/s3/v1"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
	"github.com/transferia/transferia/tests/helpers"
	"go.ytsaurus.tech/yt/go/schema"
)

var (
	Source = provider_postgres.PgSource{
		ClusterID: os.Getenv("PG_CLUSTER_ID"),
		Hosts:     []string{"localhost"},
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      helpers.GetIntFromEnv("PG_LOCAL_PORT"),
		DBTables:  []string{"public.__test1"},
	}
	Target = &s3_v1_model.S3Destination{
		Bucket:         "cleanup-replace-test",
		Cleanup:        model.Replace,
		SerializerType: model.ParsingFormatJSON,
		Serializer:     s3_v1_model.SerializerUnion{Json: &s3_v1_model.JsonSerializerConfig{}},
		Connection: s3_model.ConnectionConfig{
			AccessKey:        "1234567890",
			SecretKey:        "abcdefabcdef",
			S3ForcePathStyle: true,
			Region:           "eu-central1",
		},
	}
)

func init() {
	_ = os.Setenv("YC", "1")
	Source.WithDefaults()
}

func newS3Session(t *testing.T) *aws_s3.S3 {
	t.Helper()
	cfg := Target.Connection
	sess, err := aws_session.NewSession(&aws.Config{
		Endpoint:         aws.String(cfg.Endpoint),
		Region:           aws.String(cfg.Region),
		S3ForcePathStyle: aws.Bool(cfg.S3ForcePathStyle),
		Credentials:      aws_credentials.NewStaticCredentials(cfg.AccessKey, string(cfg.SecretKey), ""),
	})
	require.NoError(t, err)
	return aws_s3.New(sess)
}

func createBucket(t *testing.T) {
	t.Helper()
	_, err := newS3Session(t).CreateBucket(&aws_s3.CreateBucketInput{
		Bucket: aws.String(Target.Bucket),
	})
	require.NoError(t, err)
}

func countObjects(t *testing.T) int {
	t.Helper()
	out, err := newS3Session(t).ListObjects(&aws_s3.ListObjectsInput{
		Bucket: aws.String(Target.Bucket),
	})
	require.NoError(t, err)
	logger.Log.Infof("objects in bucket: %d", len(out.Contents))
	for _, obj := range out.Contents {
		logger.Log.Infof("  key: %s", aws.StringValue(obj.Key))
	}
	return len(out.Contents)
}

func TestReplaceCleanup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
		))
	}()

	Target.WithDefaults()

	if port := os.Getenv("S3MDS_PORT"); port != "" {
		Target.Connection.Endpoint = fmt.Sprintf("http://localhost:%s", port)
		createBucket(t)
	}

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotOnly)

	helpers.ActivateWithCustomTask(t, transfer, &model.TransferOperation{
		CreatedAt: time.Unix(1700000000, 0),
	})
	firstCount := countObjects(t)
	require.Positive(t, firstCount, "expected objects after first activation")

	srcConn, err := provider_postgres.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	ctx := context.Background()
	_, err = srcConn.Exec(ctx, "INSERT INTO public.__test1 (id, name) VALUES (3, 'yyyyy') ON CONFLICT DO NOTHING")
	require.NoError(t, err)
	_, err = srcConn.Exec(ctx, "UPDATE public.__test1 SET name = 'zzzzz' WHERE id = 1")
	require.NoError(t, err)

	helpers.ActivateWithCustomTask(t, transfer, &model.TransferOperation{
		CreatedAt: time.Unix(1700000001, 0),
	})
	secondCount := countObjects(t)
	require.Equal(t, firstCount, secondCount, "Replace must not accumulate objects across activations")

	s3Src := &s3_model.S3Source{
		Bucket:           Target.Bucket,
		ConnectionConfig: Target.Connection,
		TableName:        "public.__test1",
		TableNamespace:   "public",
		InputFormat:      model.ParsingFormatJSONLine,
		Format: s3_model.Format{
			JSONLSetting: &s3_model.JSONLSetting{
				NewlinesInValue:         true,
				BlockSize:               1 * 1024 * 1024,
				UnexpectedFieldBehavior: s3_model.Error,
			},
		},
		OutputSchema: []abstract.ColSchema{
			abstract.NewColSchema("id", schema.TypeInt64, true),
			abstract.NewColSchema("name", schema.TypeString, false),
		},
	}

	s3Storage, err := s3_storage.New(s3Src, transfer.ID, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	loadedItems := make([]abstract.ChangeItem, 0)

	fileList := []string{"public/__test1/part-1700000001-c21f969b.00000.json"}
	marshalledFileList, err := json.Marshal(fileList)
	require.NoError(t, err)

	err = s3Storage.LoadTable(ctx, abstract.TableDescription{
		Schema: "public",
		Name:   "public.__test1",
		Filter: abstract.WhereStatement(marshalledFileList),
	}, func(items []abstract.ChangeItem) error {
		loadedItems = append(loadedItems, items...)
		return nil
	})
	require.NoError(t, err)

	loadedValues := make(map[int]string)
	for _, item := range loadedItems {
		if !item.IsRowEvent() {
			continue
		}
		vals := item.AsMap()
		id, ok := vals["id"]
		require.True(t, ok, "id missing in item: %#v", vals)
		name, ok := vals["name"]
		require.True(t, ok, "name missing in item: %#v", vals)

		loadedValues[int(id.(int64))] = name.(string)
	}

	require.Equal(t, map[int]string{
		1: "zzzzz",
		2: "xxxxxxxxx",
		3: "yyyyy",
	}, loadedValues)
}
