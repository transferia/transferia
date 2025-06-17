//go:build !disable_s3_provider

package objectfetcher

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

type ObjectFetcherType int64

const (
	Sqs ObjectFetcherType = iota
	Sns
	PubSub
	Poller
)

func DeriveObjectFetcherType(srcModel *s3.S3Source) ObjectFetcherType {
	if srcModel.EventSource.SQS != nil && srcModel.EventSource.SQS.QueueName != "" {
		return Sqs
	} else if srcModel.EventSource.SNS != nil {
		return Sns
	} else if srcModel.EventSource.PubSub != nil {
		return PubSub
	} else {
		return Poller
	}
}

func New(
	ctx context.Context,
	logger log.Logger,
	srcModel *s3.S3Source,
	s3client s3iface.S3API,
	cp coordinator.Coordinator,
	transferID string,
	sess *session.Session,
	runtimeParallelism abstract.ShardingTaskRuntime,
	isInitFromState bool,
) (ObjectFetcher, error) {
	if srcModel == nil {
		return nil, xerrors.New("missing configuration")
	}

	switch DeriveObjectFetcherType(srcModel) {
	case Sqs:
		source, err := NewObjectFetcherSQS(ctx, logger, srcModel, sess)
		if err != nil {
			return nil, xerrors.Errorf("failed to initialize new sqs source: %w", err)
		}
		return NewObjectFetcherContractor(source), nil
	case Sns:
		return nil, xerrors.New("not yet implemented SNS")
	case PubSub:
		return nil, xerrors.New("not yet implemented PubSub")
	case Poller:
		logger.Infof("will create object_fetcher_poller, current_worker_num:%d, total_workers_num:%d", runtimeParallelism.CurrentJobIndex(), runtimeParallelism.ReplicationWorkersNum())
		source, err := NewObjectFetcherPoller(ctx, logger, srcModel, s3client, cp, transferID, runtimeParallelism.CurrentJobIndex(), runtimeParallelism.ReplicationWorkersNum(), isInitFromState)
		if err != nil {
			return nil, xerrors.Errorf("failed to initialize polling source: %w", err)
		}
		return NewObjectFetcherContractor(source), nil
	default:
		return nil, xerrors.Errorf("unknown object fetcher type: %v", DeriveObjectFetcherType(srcModel))
	}
}

func NewWrapper(
	ctx context.Context,
	srcModel *s3.S3Source,
	transferID string,
	logger log.Logger,
	registry metrics.Registry,
	cp coordinator.Coordinator,
	runtimeParallelism abstract.ShardingTaskRuntime,
	isInitFromState bool,
) (ObjectFetcher, context.Context, func(), reader.Reader, *stats.SourceStats, error) {
	sess, err := s3.NewAWSSession(logger, srcModel.Bucket, srcModel.ConnectionConfig)
	if err != nil {
		return nil, nil, nil, nil, nil, xerrors.Errorf("failed to create aws session: %w", err)
	}

	currMetrics := stats.NewSourceStats(registry)
	currReader, err := reader.New(srcModel, logger, sess, currMetrics)
	if err != nil {
		return nil, nil, nil, nil, nil, xerrors.Errorf("unable to create reader: %w", err)
	}

	outCtx, cancel := context.WithCancel(ctx)

	s3client := aws_s3.New(sess)

	fetcher, err := New(ctx, logger, srcModel, s3client, cp, transferID, sess, runtimeParallelism, isInitFromState)
	if err != nil {
		cancel()
		return nil, nil, nil, nil, nil, xerrors.Errorf("failed to initialize new object fetcher: %w", err)
	}

	return fetcher, outCtx, cancel, currReader, currMetrics, nil
}

func FetchAndCommit(
	ctx context.Context,
	srcModel *s3.S3Source,
	transferID string,
	logger log.Logger,
	registry metrics.Registry,
	cp coordinator.Coordinator,
	runtimeParallelism abstract.ShardingTaskRuntime,
	isInitFromState bool,
) error {
	poller, _, _, currReader, _, err := NewWrapper(ctx, srcModel, transferID, logger, registry, cp, runtimeParallelism, isInitFromState)
	if err != nil {
		return xerrors.Errorf("failed to create object fetcher, err: %w", err)
	}
	err = poller.FetchAndCommitAll(currReader)
	if err != nil {
		return xerrors.Errorf("failed to commit objects: %w", err)
	}
	return nil
}
