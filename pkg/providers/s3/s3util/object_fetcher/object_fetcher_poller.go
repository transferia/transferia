package object_fetcher

import (
	"context"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/logging/batching_logger"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/coordinator_utils"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/dispatcher"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/effective_worker_num"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/file"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/list"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/lr_window/r_window"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ ObjectFetcher = (*ObjectFetcherPoller)(nil)

type ObjectFetcherPoller struct {
	// input
	ctx                     context.Context
	logger                  log.Logger
	srcModel                *s3.S3Source
	s3client                s3iface.S3API                           // stored here only to be passed to ListNewMyFiles
	coordinatorStateAdapter *coordinator_utils.TransferStateAdapter // stored here to make 'SetTransferState'

	// state
	dispatcher *dispatcher.Dispatcher
}

func (s *ObjectFetcherPoller) RunBackgroundThreads(_ chan error) {}

// FetchObjects fetches objects from an S3 bucket and extracts the new objects in need of syncing from this list.
// The last synced object is used as reference to identify new objects. The newest object is added to the internal state for storing.
// All objects not matching the object type or pathPrefix are skipped.
func (s *ObjectFetcherPoller) FetchObjects(inReader reader.Reader) ([]file.File, error) {
	err := s.dispatcher.BeforeListing()
	if err != nil {
		return nil, xerrors.Errorf("contract is broken, err: %w", err)
	}

	err = list.ListNewMyFiles(
		s.ctx,
		s.logger,
		s.srcModel,
		inReader,
		s.s3client,
		s.dispatcher,
	)
	if err != nil {
		return nil, xerrors.Errorf("unable to list objects, err: %w", err)
	}

	s.dispatcher.AfterListing()

	return s.dispatcher.ExtractSortedFileEntries(), nil
}

func (s *ObjectFetcherPoller) Commit(fileName string) error {
	err := s.dispatcher.Commit(fileName)
	if err != nil {
		return xerrors.Errorf("unable to commit object, err: %w", err)
	}

	state := s.dispatcher.SerializeState()
	err = s.coordinatorStateAdapter.SetTransferState(state) // TODO - wrap into retries?
	if err != nil {
		return xerrors.Errorf("unable to set transfer state, err: %w", err)
	}
	s.logger.Info("set state successfully")

	return nil
}

func (s *ObjectFetcherPoller) Close() error {
	return s.coordinatorStateAdapter.Flush(time.Now())
}

func initDispatcherFromState(
	logger log.Logger,
	coordinatorStateAdapter *coordinator_utils.TransferStateAdapter,
	srcModel *s3.S3Source,
	inDispatcher *dispatcher.Dispatcher,
) error {
	stateMap, err := coordinatorStateAdapter.GetTransferState()
	if err != nil {
		return xerrors.Errorf("unable to get transfer state: %w", err)
	}
	batching_logger.LogLine(func(in string) { logger.Info(in) }, "load state", log.Any("state", stateMap))
	for k, v := range stateMap {
		kNum, err := strconv.Atoi(k)
		if err != nil {
			continue
		}
		if inDispatcher.IsMySyntheticPartitionNum(kNum) {
			stateStr, ok := v.Generic.(string)
			if !ok {
				logger.Warnf("unable to convert generic to string, 'Generic' type: %T, err: %v", v.Generic, err)
			}
			err := inDispatcher.InitSyntheticPartitionNumByState(kNum, stateStr)
			if err != nil {
				return xerrors.Errorf("unable to init synthetic_partition state, err: %w", err)
			}
		}
	}
	return nil
}

func newObjectFetcherPoller(
	ctx context.Context,
	logger log.Logger,
	srcModel *s3.S3Source,
	s3client s3iface.S3API,
	coordinatorStateAdapter *coordinator_utils.TransferStateAdapter,
	effectiveWorkerNum *effective_worker_num.EffectiveWorkerNum,
	rWindow *r_window.RWindow,
) (*ObjectFetcherPoller, error) {
	currDispatcher := dispatcher.NewDispatcher(rWindow, srcModel.SyntheticPartitionsNum, effectiveWorkerNum, srcModel.OverlapDuration)

	logger.Infof(
		"worker %d/%d (for %d synthetic_partitions) took next synthetic_partitions: %v",
		effectiveWorkerNum.CurrentWorkerNum,
		effectiveWorkerNum.WorkersCount,
		srcModel.SyntheticPartitionsNum,
		currDispatcher.MySyntheticPartitionNums(),
	)

	err := initDispatcherFromState(logger, coordinatorStateAdapter, srcModel, currDispatcher)
	if err != nil {
		return nil, xerrors.Errorf("unable to init dispatcher, err: %w", err)
	}

	return &ObjectFetcherPoller{
		ctx:                     ctx,
		logger:                  logger,
		srcModel:                srcModel,
		s3client:                s3client,
		coordinatorStateAdapter: coordinatorStateAdapter,
		dispatcher:              currDispatcher,
	}, nil
}
