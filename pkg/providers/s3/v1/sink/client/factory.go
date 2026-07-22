package client

import (
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/transferia/transferia/library/go/core/xerrors"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/s3sess"
	"go.ytsaurus.tech/library/go/core/log"
)

func New(
	lgr log.Logger,
	bucket string,
	connection s3_model.ConnectionConfig,
	partSize int64,
	useReplace bool,
) (S3Client, error) {
	sess, err := s3sess.NewAWSSession(lgr, bucket, connection)
	if err != nil {
		return nil, xerrors.Errorf("unable to create session to s3 bucket: %w", err)
	}

	raw := aws_s3.New(sess)
	uploadAPI := s3iface.S3API(raw)
	if useReplace {
		uploadAPI = &replaceS3API{S3API: raw}
	}

	uploader := s3manager.NewUploaderWithClient(uploadAPI)
	uploader.PartSize = partSize

	return newS3ClientImpl(raw, uploader), nil
}
