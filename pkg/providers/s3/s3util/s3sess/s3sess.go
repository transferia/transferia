package s3sess

import (
	aws_session "github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	s3_reader_registry "github.com/transferia/transferia/pkg/providers/s3/reader/registry"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewSessClientReaderMetrics(logger log.Logger, srcModel *s3_model.S3Source, registry core_metrics.Registry) (*aws_session.Session, *aws_s3.S3, s3_reader.Reader, *stats.SourceStats, error) {
	sess, err := NewAWSSession(logger, srcModel.Bucket, srcModel.ConnectionConfig)
	if err != nil {
		return nil, nil, nil, nil, xerrors.Errorf("failed to create aws session: %w", err)
	}

	currMetrics := stats.NewSourceStats(registry)
	currReader, err := s3_reader_registry.NewReader(srcModel, logger, sess, currMetrics, s3raw.NewRealS3RawReaderBuilder())
	if err != nil {
		return nil, nil, nil, nil, xerrors.Errorf("unable to create reader: %w", err)
	}

	s3client := aws_s3.New(sess)

	return sess, s3client, currReader, currMetrics, nil
}
