//go:build !disable_s3_coordinator

package s3coordinator

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/tests/tcrecipes"
	"github.com/transferia/transferia/tests/tcrecipes/objectstorage"
	"go.ytsaurus.tech/library/go/core/log"
)

func envOrDefault(key, def string) string {
	if r, ok := os.LookupEnv(key); ok {
		return r
	}
	return def
}

func NewS3Recipe(bucket string) (*CoordinatorS3, error) {
	if tcrecipes.Enabled() {
		_, err := objectstorage.Prepare(context.Background())
		if err != nil {
			return nil, xerrors.Errorf("unable to prepare recipe: %w", err)
		}
	}
	// infer args from env
	endpoint := envOrDefault("S3_ENDPOINT", fmt.Sprintf("http://localhost:%v", os.Getenv("S3MDS_PORT")))
	region := envOrDefault("S3_REGION", "ru-central1")
	accessKey := envOrDefault("S3_ACCESS_KEY", "1234567890")
	secret := envOrDefault("S3_SECRET", "abcdefabcdef")

	if bucket == "" {
		bucket = "coordinator"
		sess, err := session.NewSession(&aws.Config{
			Endpoint:         aws.String(endpoint),
			Region:           aws.String(region),
			S3ForcePathStyle: aws.Bool(true),
			Credentials: credentials.NewStaticCredentials(
				accessKey, secret, "",
			),
		})
		if err != nil {
			return nil, xerrors.Errorf("unable to init session: %w", err)
		}
		res, err := s3.New(sess).CreateBucket(&s3.CreateBucketInput{
			Bucket: aws.String(bucket),
		})
		// No need to check error because maybe the bucket already exists
		logger.Log.Info("create bucket result", log.Any("res", res), log.Error(err))
	}
	cp, err := NewS3(
		bucket,
		logger.Log,
		&aws.Config{
			Region:           aws.String(region),
			Credentials:      credentials.NewStaticCredentials(accessKey, secret, ""),
			Endpoint:         aws.String(endpoint),
			S3ForcePathStyle: aws.Bool(true), // Enable path-style access
		},
	)
	if err != nil {
		return nil, xerrors.Errorf("unable to create s3 coordinator: %w", err)
	}
	return cp, nil
}
