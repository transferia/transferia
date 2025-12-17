package object_fetcher

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/logging/batching_logger"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/coordinator_utils"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/effective_worker_num"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/list"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/lr_window/r_window"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/s3sess"
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

// replication
func newObjectFetcher(
	ctx context.Context,
	logger log.Logger,
	srcModel *s3.S3Source,
	s3client s3iface.S3API,
	coordinatorStateAdapter *coordinator_utils.TransferStateAdapter,
	sess *session.Session,
	runtime abstract.ShardingTaskRuntime,
	rWindow *r_window.RWindow,
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
		return source, nil
	case Sns:
		return nil, xerrors.New("not yet implemented SNS")
	case PubSub:
		return nil, xerrors.New("not yet implemented PubSub")
	case Poller:
		logger.Infof("will create object_fetcher_poller, current_worker_num:%d, total_workers_num:%d", runtime.CurrentJobIndex(), runtime.ReplicationWorkersNum())
		effectiveWorkerNum, err := effective_worker_num.NewEffectiveWorkerNum(logger, runtime, false)
		if err != nil {
			return nil, xerrors.Errorf("unable to determine current and max worker num, err: %w", err)
		}
		source, err := newObjectFetcherPoller(
			ctx,
			logger,
			srcModel,
			s3client,
			coordinatorStateAdapter,
			effectiveWorkerNum,
			rWindow,
		)
		if err != nil {
			return nil, xerrors.Errorf("failed to initialize polling source: %w", err)
		}
		return source, nil
	default:
		return nil, xerrors.Errorf("unknown ObjectFetcher type: %v", DeriveObjectFetcherType(srcModel))
	}
}

// replication
func newObjectFetcherWrapped(
	ctx context.Context,
	logger log.Logger,
	srcModel *s3.S3Source,
	s3client s3iface.S3API,
	coordinatorStateAdapter *coordinator_utils.TransferStateAdapter,
	sess *session.Session,
	runtimeParallelism abstract.ShardingTaskRuntime,
	rWindow *r_window.RWindow,
) (ObjectFetcher, error) {
	objectFetcher, err := newObjectFetcher(ctx, logger, srcModel, s3client, coordinatorStateAdapter, sess, runtimeParallelism, rWindow)
	if err != nil {
		return nil, xerrors.Errorf("unable to create ObjectFetcher, err: %w", err)
	}
	return NewObjectFetcherContractor(objectFetcher), nil
}

// main factory function
// used in:
//   - s3 source
func NewWrapped(
	ctx context.Context,
	logger log.Logger,
	registry metrics.Registry,
	srcModel *s3.S3Source,
	transferID string,
	cp coordinator.Coordinator,
	runtimeParallelism abstract.ShardingTaskRuntime,
) (ObjectFetcher, context.Context, func(), reader.Reader, *stats.SourceStats, error) {
	sess, s3client, currReader, currMetrics, err := s3sess.NewSessClientReaderMetrics(logger, srcModel, registry)
	if err != nil {
		return nil, nil, nil, nil, nil, xerrors.Errorf("failed to create s3session/s3client/reader, err: %w", err)
	}

	outCtx, cancel := context.WithCancel(ctx)

	coordinatorStateAdapter := coordinator_utils.NewTransferStateAdapter(cp, srcModel.ThrottleCPDuration, transferID)

	objectFetcher, err := newObjectFetcherWrapped(ctx, logger, srcModel, s3client, coordinatorStateAdapter, sess, runtimeParallelism, r_window.NewRWindowEmpty(srcModel.OverlapDuration))
	if err != nil {
		cancel()
		return nil, nil, nil, nil, nil, xerrors.Errorf("failed to initialize new object objectFetcher: %w", err)
	}

	return objectFetcher, outCtx, cancel, currReader, currMetrics, nil
}

// snapshot
// used in:
//   - s3_shared_memory_secondary_worker
func NewObjectFetcherPollerWrapped(
	ctx context.Context,
	logger log.Logger,
	srcModel *s3.S3Source,
	s3client s3iface.S3API,
	coordinatorStateAdapter *coordinator_utils.TransferStateAdapter,
	effectiveWorkerNum *effective_worker_num.EffectiveWorkerNum,
	rWindow *r_window.RWindow,
) (ObjectFetcher, error) {
	result, err := newObjectFetcherPoller(ctx, logger, srcModel, s3client, coordinatorStateAdapter, effectiveWorkerNum, rWindow)
	if err != nil {
		return nil, xerrors.Errorf("failed to create new ObjectFetcher poller, err: %w", err)
	}
	return NewObjectFetcherContractor(result), nil
}

// used in:
//   - 'activate' on REPLICATION_ONLY - to commit all known files
func FetchAndCommit(
	ctx context.Context,
	srcModel *s3.S3Source,
	transferID string,
	logger log.Logger,
	registry metrics.Registry,
	cp coordinator.Coordinator,
) error {
	dispatcher, err := list.ListAllReturnDispatcher(ctx, logger, registry, srcModel)
	if err != nil {
		return xerrors.Errorf("unable to list objects, err: %w", err)
	}
	err = dispatcher.CommitAll()
	if err != nil {
		return xerrors.Errorf("unable to commit objects, err: %w", err)
	}
	currState := dispatcher.SerializeState()
	batching_logger.LogLineInfo(func(in string) { logger.Info(in) }, "state serialized (bcs commit_all)", log.Any("state", currState))
	logger.Info("will set state")

	coordinatorStateAdapter := coordinator_utils.NewTransferStateAdapter(cp, srcModel.ThrottleCPDuration, transferID)
	err = coordinatorStateAdapter.SetTransferState(currState) // TODO - wrap into retries?
	if err != nil {
		return xerrors.Errorf("unable to set transfer state, err: %w", err)
	}
	logger.Info("set state successfully")
	return nil
}
