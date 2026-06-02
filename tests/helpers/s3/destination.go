package s3

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
	_ "github.com/transferia/transferia/pkg/providers/s3/provider"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
)

func WaitForDestinationData(t *testing.T, dst *s3_v1_model.S3Destination, expectedFileCount int) map[string][]byte {
	session, err := aws_session.NewSession(&aws.Config{
		Endpoint:         aws.String(dst.Connection.Endpoint),
		Region:           aws.String(dst.Connection.Region),
		S3ForcePathStyle: aws.Bool(dst.Connection.S3ForcePathStyle),
		Credentials: aws_credentials.NewStaticCredentials(
			dst.Connection.AccessKey, string(dst.Connection.SecretKey), "",
		),
	})
	require.NoError(t, err)
	s3client := aws_s3.New(session)

	var objects *aws_s3.ListObjectsOutput
	for {
		time.Sleep(time.Second * 3)

		objects, err = s3client.ListObjects(&aws_s3.ListObjectsInput{
			Bucket: aws.String(dst.Bucket),
		})
		require.NoError(t, err)
		logger.Log.Infof("Found %d objects in bucket", len(objects.Contents))

		if len(objects.Contents) >= expectedFileCount {
			break
		}
	}

	fileToData := make(map[string][]byte)
	for _, obj := range objects.Contents {
		getResult, err := s3client.GetObject(&aws_s3.GetObjectInput{
			Bucket: aws.String(dst.Bucket),
			Key:    obj.Key,
		})
		require.NoError(t, err)

		data, err := io.ReadAll(getResult.Body)
		require.NoError(t, err)
		_ = getResult.Body.Close()

		fileToData[*obj.Key] = data
	}

	return fileToData
}
