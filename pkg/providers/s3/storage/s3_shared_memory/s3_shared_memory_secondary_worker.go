package s3_shared_memory

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/logging/batching_logger"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/coordinator_utils"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/effective_worker_num"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/file"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/lr_window/r_window"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/object_fetcher"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/s3sess"
	"go.ytsaurus.tech/library/go/core/log"
)

type S3SharedMemorySecondaryWorker struct {
	cfg                *s3.S3Source
	transferID         string
	effectiveWorkerNum *effective_worker_num.EffectiveWorkerNum
	cp                 coordinator.Coordinator

	poller object_fetcher.ObjectFetcher // poller wrapped over contractor
	files  []file.File
}

func (m *S3SharedMemorySecondaryWorker) Store([]*abstract.OperationTablePart) error {
	panic("not implemented") // never should be called on secondary-worker
}

// TAKE task
func (m *S3SharedMemorySecondaryWorker) NextOperationTablePart(ctx context.Context) (*abstract.OperationTablePart, error) {
	if len(m.files) == 0 {
		err := coordinator_utils.SetWorkerDone(m.cp, m.transferID, m.effectiveWorkerNum)
		if err != nil {
			return nil, xerrors.Errorf("unable to reset workers state, err: %w", err)
		}
		return nil, nil
	}
	result := m.files[0]
	m.files = m.files[1:]

	valBytes, err := json.Marshal([]string{result.FileName})
	if err != nil {
		return nil, xerrors.Errorf("unable to marshal result, err: %w", err)
	}

	return &abstract.OperationTablePart{
		OperationID:   "",
		Schema:        m.cfg.TableNamespace,
		Name:          m.cfg.TableName,
		Offset:        uint64(0),
		Filter:        string(valBytes),
		PartsCount:    uint64(0),
		PartIndex:     uint64(0),
		WorkerIndex:   nil,
		ETARows:       uint64(0),
		CompletedRows: uint64(0),
		ReadBytes:     uint64(0),
		Completed:     false,
	}, nil
}

// COMMIT task
func (m *S3SharedMemorySecondaryWorker) UpdateOperationTablesParts(operationID string, tables []*abstract.OperationTablePart) error {
	if len(tables) != 1 {
		return xerrors.Errorf("S3SharedMemorySecondaryWorker.UpdateOperationTablesParts - impossible situation, len(tables) can't not be equal to 1")
	}
	return m.poller.Commit(tables[0].Filter)
}

func (m *S3SharedMemorySecondaryWorker) Close() error {
	return m.poller.Close()
}

//---------------------------------------------------------------------------------------------------------------------
// metrika-trash useless things

func (m *S3SharedMemorySecondaryWorker) GetShardStateNoWait(ctx context.Context, operationID string) (string, error) {
	panic("not implemented")
}

func (m *S3SharedMemorySecondaryWorker) SetOperationState(operationID string, newState string) error {
	panic("not implemented")
}

//---------------------------------------------------------------------------------------------------------------------

func NewS3SharedMemorySecondaryWorker(
	ctx context.Context,
	logger log.Logger,
	registry metrics.Registry,
	cfg *s3.S3Source,
	transferID string,
	runtime abstract.Runtime,
	cp coordinator.Coordinator,
	shardingContext []byte,
) (*S3SharedMemorySecondaryWorker, error) {
	_, s3client, currReader, _, err := s3sess.NewSessClientReaderMetrics(logger, cfg, registry)
	if err != nil {
		return nil, xerrors.Errorf("failed to create s3session/s3client/reader, err: %w", err)
	}

	// parallelism

	runtimeUnwrapped, ok := runtime.(abstract.ShardingTaskRuntime)
	if !ok {
		return nil, xerrors.Errorf("runtime is unsupported type, %T", runtime)
	}
	effectiveWorkerNum, err := effective_worker_num.NewEffectiveWorkerNum(logger, runtimeUnwrapped, true)
	if err != nil {
		return nil, xerrors.Errorf("unable to determine current and max worker num, err: %w", err)
	}

	rWindow, err := r_window.NewRWindowFromSerialized(cfg.OverlapDuration, shardingContext)
	if err != nil {
		return nil, xerrors.Errorf("unable to create rWindow, err: %w", err)
	}

	coordinatorStateAdapter := coordinator_utils.NewTransferStateAdapter(cp, cfg.ThrottleCPDuration, transferID)

	poller, err := object_fetcher.NewObjectFetcherPollerWrapped(
		ctx,
		logger,
		cfg,
		s3client,
		coordinatorStateAdapter,
		effectiveWorkerNum,
		rWindow,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to create poller for files, err: %w", err)
	}

	files, err := poller.FetchObjects(currReader)
	if err != nil {
		return nil, xerrors.Errorf("failed to fetch files, err: %w", err)
	}

	batching_logger.LogLines(logger, fmt.Sprintf("found %d files: ", len(files)), file.FileArr(files))

	return &S3SharedMemorySecondaryWorker{
		cfg:                cfg,
		transferID:         transferID,
		effectiveWorkerNum: effectiveWorkerNum,
		cp:                 cp,
		poller:             poller,
		files:              files,
	}, nil
}
