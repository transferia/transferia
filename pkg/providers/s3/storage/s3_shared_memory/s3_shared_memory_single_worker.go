package s3_shared_memory

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/logging/batching_logger"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/file"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/list"
	"go.ytsaurus.tech/library/go/core/log"
)

type S3SharedMemorySingleWorker struct {
	cfg *s3.S3Source

	objects []file.File
}

func (m *S3SharedMemorySingleWorker) Store([]*abstract.OperationTablePart) error {
	return nil // called from BuildTPP->NewTPPSetter, but there should be nothing useful
}

// TAKE task
func (m *S3SharedMemorySingleWorker) NextOperationTablePart(ctx context.Context) (*abstract.OperationTablePart, error) {
	if len(m.objects) == 0 {
		return nil, nil
	}
	result := m.objects[0]
	m.objects = m.objects[1:]

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
func (m *S3SharedMemorySingleWorker) UpdateOperationTablesParts(operationID string, tables []*abstract.OperationTablePart) error {
	return nil // NO-OP
}

func (m *S3SharedMemorySingleWorker) Close() error {
	return nil
}

//---------------------------------------------------------------------------------------------------------------------
// metrika-trash useless things

func (m *S3SharedMemorySingleWorker) GetShardStateNoWait(ctx context.Context, operationID string) (string, error) {
	panic("not implemented")
}

func (m *S3SharedMemorySingleWorker) SetOperationState(operationID string, newState string) error {
	panic("not implemented")
}

//---------------------------------------------------------------------------------------------------------------------

func NewS3SharedMemorySingleWorker(
	ctx context.Context,
	logger log.Logger,
	registry metrics.Registry,
	cfg *s3.S3Source,
) (*S3SharedMemorySingleWorker, error) {
	logger.Info("list all files in s3 - started")

	files, err := list.ListAll(ctx, logger, registry, cfg)
	if err != nil {
		return nil, xerrors.Errorf("unable to list all, err: %w", err)
	}

	logger.Infof("listed all files in s3 - finished, found %d files", len(files))
	batching_logger.LogLines(logger, fmt.Sprintf("found %d files: ", len(files)), file.FileArr(files))

	return &S3SharedMemorySingleWorker{
		cfg:     cfg,
		objects: files,
	}, nil
}
