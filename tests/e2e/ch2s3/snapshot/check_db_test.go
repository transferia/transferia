package snapshot

import (
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	chrecipe "github.com/transferria/transferria/pkg/providers/clickhouse/recipe"
	s3_provider "github.com/transferria/transferria/pkg/providers/s3"
	"github.com/transferria/transferria/tests/helpers"
)

var (
	testBucket   = s3_provider.EnvOrDefault("TEST_BUCKET", "barrel")
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = *chrecipe.MustSource(chrecipe.WithInitFile("dump/src.sql"), chrecipe.WithDatabase("clickhouse_test"))
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

	// After load data into s3
	require.Len(t, objects.Contents, 1)
	obj, err := s3.New(sess).GetObject(&s3.GetObjectInput{Bucket: &s3Target.Bucket, Key: objects.Contents[0].Key})
	require.NoError(t, err)

	data, err := io.ReadAll(obj.Body)
	require.NoError(t, err)
	logger.Log.Infof("object: %v content:\n%v", *objects.Contents[0].Key, string(data))
}
