package s3

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	aws_credentials "github.com/aws/aws-sdk-go/aws/credentials"
	aws_session "github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	_ "github.com/transferia/transferia/pkg/providers/s3/provider"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
	"go.ytsaurus.tech/library/go/core/log"
)

func CreateBucket(t *testing.T, cfg *s3_v1_model.S3Destination) {
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
