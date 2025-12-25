package s3sess

import (
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
	reader_factory "github.com/transferia/transferia/pkg/providers/s3/reader/registry"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewSessClientReaderMetrics(logger log.Logger, srcModel *s3.S3Source, registry metrics.Registry) (*session.Session, *aws_s3.S3, reader.Reader, *stats.SourceStats, error) {
	sess, err := s3.NewAWSSession(logger, srcModel.Bucket, srcModel.ConnectionConfig)
	if err != nil {
		return nil, nil, nil, nil, xerrors.Errorf("failed to create aws session: %w", err)
	}

	currMetrics := stats.NewSourceStats(registry)
	currReader, err := reader_factory.NewReader(srcModel, logger, sess, currMetrics)
	if err != nil {
		return nil, nil, nil, nil, xerrors.Errorf("unable to create reader: %w", err)
	}

	s3client := aws_s3.New(sess)

	return sess, s3client, currReader, currMetrics, nil
}
