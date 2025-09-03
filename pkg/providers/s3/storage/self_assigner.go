package storage

import (
	"context"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/s3"
	objectfetcher "github.com/transferia/transferia/pkg/providers/s3/source/object_fetcher"
	"github.com/transferia/transferia/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
)

type SelfOperationTablePartAssigner struct {
	objectFetcher objectfetcher.ObjectFetcher
	files         *set.Set[string]
}

func (a *SelfOperationTablePartAssigner) NextOperationTablePart() (*abstract.OperationTablePart, error) {
	if a.files.Empty() {
		return nil, nil
	}
	nextFileName := a.files.Slice()[0]
	a.files.Remove(nextFileName)
	return &abstract.OperationTablePart{
		//Filter: nextFileName,
		//
		OperationID:   "",
		Schema:        "",
		Name:          "",
		Offset:        uint64(0),
		Filter:        "",
		PartsCount:    uint64(0),
		PartIndex:     uint64(0),
		WorkerIndex:   nil,
		ETARows:       uint64(0),
		CompletedRows: uint64(0),
		ReadBytes:     uint64(0),
		Completed:     false,
	}, nil // TODO - make some conversation
}

func (a *SelfOperationTablePartAssigner) Commit(el *abstract.OperationTablePart) error {
	return a.objectFetcher.Commit(el.Filter) // TODO - decode filename from
}

func NewSelfOperationTablePartAssigner(
	ctx context.Context,
	logger log.Logger,
	client s3iface.S3API,
	cp coordinator.Coordinator,
	srcModel *s3.S3Source,
	transferID string,
	runtimeParallelism abstract.ShardingTaskRuntime,
	isIncremental bool,
) (*SelfOperationTablePartAssigner, error) {
	objectFetcher, _, _, currReader, _, err := objectfetcher.NewWrapper(
		ctx,
		srcModel,
		transferID,
		logger,
		nil,
		cp,
		runtimeParallelism,
		isIncremental,
	)
	if err != nil {
		return nil, xerrors.Errorf("Failed to create object fetcher: %w", err)
	}
	files, err := objectFetcher.FetchObjects(currReader)
	if err != nil {
		return nil, xerrors.Errorf("Failed to fetch objects: %w", err)
	}
	return &SelfOperationTablePartAssigner{
		objectFetcher: objectFetcher,
		files:         set.New(files...),
	}, nil
}
