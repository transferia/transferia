package snapshot

import (
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	aws_credentials "github.com/aws/aws-sdk-go/aws/credentials"
	aws_session "github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/chrecipe"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"github.com/transferia/transferia/pkg/providers/s3/s3recipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	testBucket   = s3recipe.EnvOrDefault("TEST_BUCKET", "barrel")
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = *chrecipe.MustSource(chrecipe.WithInitFile("dump/src.sql"), chrecipe.WithDatabase("clickhouse_test"))
)

func TestSnapshotParquet(t *testing.T) {
	s3Target := s3recipe.PrepareS3(t, testBucket, model.ParsingFormatPARQUET, s3_model.GzipEncoding)
	s3Target.WithDefaults()

	require.NoError(t, network.CheckConnections(
		network.LabeledPort{Label: "CH source", Port: Source.NativePort},
	))
	Source.WithDefaults()

	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, s3Target, abstract.TransferTypeSnapshotOnly)
	// checking the bucket is empty
	sess, err := aws_session.NewSession(&aws.Config{
		Endpoint:         aws.String(s3Target.Endpoint),
		Region:           aws.String(s3Target.Region),
		S3ForcePathStyle: aws.Bool(s3Target.S3ForcePathStyle),
		Credentials: aws_credentials.NewStaticCredentials(
			s3Target.AccessKey, s3Target.Secret, "",
		),
	})
	require.NoError(t, err)

	objects, err := aws_s3.New(sess).ListObjects(&aws_s3.ListObjectsInput{Bucket: &s3Target.Bucket})
	require.NoError(t, err)

	logger.Log.Infof("objects: %v", objects.String())
	require.Len(t, objects.Contents, 0)

	time.Sleep(5 * time.Second)

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, s3Target, TransferType)
	delivery.Activate(t, transfer)

	sess, err = aws_session.NewSession(&aws.Config{
		Endpoint:         aws.String(s3Target.Endpoint),
		Region:           aws.String(s3Target.Region),
		S3ForcePathStyle: aws.Bool(s3Target.S3ForcePathStyle),
		Credentials: aws_credentials.NewStaticCredentials(
			s3Target.AccessKey, s3Target.Secret, "",
		),
	})
	require.NoError(t, err)

	objects, err = aws_s3.New(sess).ListObjects(&aws_s3.ListObjectsInput{Bucket: &s3Target.Bucket})
	require.NoError(t, err)
	logger.Log.Infof("objects: %v", objects.String())

	// After load data into s3
	require.Len(t, objects.Contents, 1)
	obj, err := aws_s3.New(sess).GetObject(&aws_s3.GetObjectInput{Bucket: &s3Target.Bucket, Key: objects.Contents[0].Key})
	require.NoError(t, err)

	data, err := io.ReadAll(obj.Body)
	require.NoError(t, err)
	logger.Log.Infof("object: %v content:\n%v", *objects.Contents[0].Key, string(data))
}
