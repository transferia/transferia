//go:build !disable_s3_provider

package objectfetcher

import (
	"context"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/source/object_fetcher/poller/dispatcher"
	"github.com/transferia/transferia/pkg/providers/s3/source/object_fetcher/poller/dispatcher/file"
	"github.com/transferia/transferia/pkg/providers/s3/source/object_fetcher/poller/list"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ ObjectFetcher = (*ObjectFetcherPoller)(nil)

type ObjectFetcherPoller struct {
	// input
	ctx        context.Context
	logger     log.Logger
	srcModel   *s3.S3Source
	s3client   s3iface.S3API           // stored here only to be passed to ListNewMyFiles
	cp         coordinator.Coordinator // stored here to make 'SetTransferState'
	transferID string                  // stored here to make 'SetTransferState'

	// state
	dispatcher *dispatcher.Dispatcher
}

func (s *ObjectFetcherPoller) RunBackgroundThreads(_ chan error) {}

// FetchObjects fetches objects from an S3 bucket and extracts the new objects in need of syncing from this list.
// The last synced object is used as reference to identify new objects. The newest object is added to the internal state for storing.
// All objects not matching the object type or pathPrefix are skipped.
func (s *ObjectFetcherPoller) FetchObjects(inReader reader.Reader) ([]string, error) {
	err := s.dispatcher.ResetBeforeListing()
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

	fileNames, err := s.dispatcher.ExtractSortedFileNames()
	if err != nil {
		return nil, xerrors.Errorf("unable to extract files, err: %w", err)
	}
	return fileNames, nil
}

func (s *ObjectFetcherPoller) Commit(fileName string) error {
	lastCommittedStateChanged, err := s.dispatcher.Commit(fileName)
	if err != nil {
		return xerrors.Errorf("unable to commit object, err: %w", err)
	}

	if lastCommittedStateChanged {
		state := s.dispatcher.SerializeState()
		s.logger.Info("state serialized (bcs commit)", log.Any("state", state))
		s.logger.Info("will set state")
		err := s.cp.SetTransferState(s.transferID, state) // TODO - wrap into retries?
		if err != nil {
			return xerrors.Errorf("unable to set transfer state, err: %w", err)
		}
		s.logger.Info("set state successfully")
	}

	return nil
}

func (s *ObjectFetcherPoller) FetchAndCommitAll(inReader reader.Reader) error {
	err := list.ListNewMyFiles(
		s.ctx,
		s.logger,
		s.srcModel,
		inReader,
		s.s3client,
		s.dispatcher,
	)
	if err != nil {
		return xerrors.Errorf("unable to list objects, err: %w", err)
	}
	err = s.dispatcher.CommitAll()
	if err != nil {
		return xerrors.Errorf("unable to commit objects, err: %w", err)
	}
	state := s.dispatcher.SerializeState()
	s.logger.Info("state serialized (bcs commit_all)", log.Any("state", state)) // TODO - log via smart logger
	s.logger.Info("will set state")
	err = s.cp.SetTransferState(s.transferID, state) // TODO - wrap into retries?
	if err != nil {
		return xerrors.Errorf("unable to set transfer state, err: %w", err)
	}
	s.logger.Info("set state successfully")
	return nil
}

func isMigrateState(srcModel *s3.S3Source, stateMap map[string]*coordinator.TransferStateData) bool {
	_, containsReadProgressKey := stateMap["ReadProgressKey"]
	return srcModel.SyntheticPartitionsNum == 1 && len(stateMap) == 1 && containsReadProgressKey
}

func migrateOldState(stateMap map[string]*coordinator.TransferStateData, inDispatcher *dispatcher.Dispatcher) error {
	oldState := stateMap["ReadProgressKey"]
	oldStateVal, ok := oldState.Generic.(map[string]interface{})
	if !ok {
		return xerrors.Errorf("unable to read old state from migrated state, type:%T", oldState.Generic)
	}
	currTime, err := time.Parse("2006-01-02T15:04:05Z", oldStateVal["last_modified"].(string)) // example: "2025-05-28T18:09:21Z"
	if err != nil {
		return xerrors.Errorf("unable to parse old state, err: %w", err)
	}
	currFile := file.NewFile(
		oldStateVal["name"].(string),
		int64(1),
		currTime,
	)
	isAdded, err := inDispatcher.AddIfNew(currFile)
	if err != nil {
		return xerrors.Errorf("unable to add new file, err: %w", err)
	}
	if !isAdded {
		return xerrors.Errorf("unable to add new file")
	}
	err = inDispatcher.CommitAll()
	if err != nil {
		return xerrors.Errorf("unable to commit objects, err: %w", err)
	}
	return nil
}

func initDispatcherFromState(logger log.Logger, cp coordinator.Coordinator, srcModel *s3.S3Source, transferID string, inDispatcher *dispatcher.Dispatcher) error {
	stateMap, err := cp.GetTransferState(transferID)
	if err != nil {
		return xerrors.Errorf("unable to get transfer state: %w", err)
	}
	logger.Info("load state", log.Any("state", stateMap)) // TODO - log via smart logger
	if isMigrateState(srcModel, stateMap) {
		return migrateOldState(stateMap, inDispatcher)
	} else {
		for k, v := range stateMap {
			kNum, err := strconv.Atoi(k)
			if err != nil {
				logger.Warnf("unable to convert transfer state to number, err: %v", err)
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
	}
	return nil
}

func NewObjectFetcherPoller(
	ctx context.Context,
	logger log.Logger,
	srcModel *s3.S3Source,
	s3client s3iface.S3API,
	cp coordinator.Coordinator,
	transferID string,
	currentWorkerNum int,
	totalWorkersNum int,
	isInitFromState bool,
) (*ObjectFetcherPoller, error) {
	workerProperties, err := dispatcher.NewWorkerProperties(currentWorkerNum, totalWorkersNum)
	if err != nil {
		return nil, xerrors.Errorf("unable to create worker properties, err: %w", err)
	}

	currDispatcher := dispatcher.NewDispatcher(srcModel.SyntheticPartitionsNum, workerProperties)
	logger.Infof("worker %d/%d (for %d synthetic_partitions) took next synthetic_partitions: %v", currentWorkerNum, totalWorkersNum, srcModel.SyntheticPartitionsNum, currDispatcher.MySyntheticPartitionNums())
	if isInitFromState {
		err = initDispatcherFromState(logger, cp, srcModel, transferID, currDispatcher)
		if err != nil {
			return nil, xerrors.Errorf("unable to init dispatcher, err: %w", err)
		}
	}

	return &ObjectFetcherPoller{
		ctx:        ctx,
		logger:     logger,
		srcModel:   srcModel,
		s3client:   s3client,
		cp:         cp,
		transferID: transferID,
		dispatcher: currDispatcher,
	}, nil
}
