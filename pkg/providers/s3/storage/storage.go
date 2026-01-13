package storage

import (
	"context"
	"encoding/json"
	"time"

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/pusher"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
	reader_factory "github.com/transferia/transferia/pkg/providers/s3/reader/registry"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/coordinator_utils"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/effective_worker_num"
	"github.com/transferia/transferia/pkg/providers/s3/storage/s3_shared_memory"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ abstract.Storage = (*Storage)(nil)

type Storage struct {
	cfg         *s3.S3Source
	transferID  string
	client      s3iface.S3API
	logger      log.Logger
	tableSchema *abstract.TableSchema
	reader      reader.Reader
	registry    metrics.Registry

	shardingContext []byte
}

func (s *Storage) Close() {
}

func (s *Storage) Ping() error {
	return nil
}

func (s *Storage) TableSchema(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
	return s.tableSchema, nil
}

func (s *Storage) LoadTable(ctx context.Context, table abstract.TableDescription, inPusher abstract.Pusher) error {
	wrappedPusher := func(items []abstract.ChangeItem) error {
		partID := table.GeneratePartID()
		for i, item := range items {
			if item.IsRowEvent() {
				items[i].Schema = s.cfg.TableNamespace
				items[i].Table = s.cfg.TableName
				items[i].PartID = partID
			}
		}
		return inPusher(items)
	}

	syncPusher := pusher.New(wrappedPusher, nil, s.logger, 0)

	// array here - to 'Metrika' source can batch many files into one 'abstract.TableDescription'
	var fileList []string
	err := json.Unmarshal([]byte(table.Filter), &fileList)
	if err != nil {
		return xerrors.Errorf("unable to unmarshal filter, val:%s, err: %w", string(table.Filter), err)
	}

	for _, currFile := range fileList {
		if err := s.reader.Read(ctx, currFile, syncPusher); err != nil {
			return xerrors.Errorf("unable to read file: %s: %w", table.Filter, err)
		}
	}
	return nil
}

func (s *Storage) TableList(_ abstract.IncludeTableList) (abstract.TableMap, error) {
	// 'tableID' here - fake, whole s3-source is like one big virtual table
	tableID := *abstract.NewTableID(s.cfg.TableNamespace, s.cfg.TableName)
	rows, err := s.EstimateTableRowsCount(tableID)
	if err != nil {
		return nil, xerrors.Errorf("failed to estimate row count: %w", err)
	}
	return map[abstract.TableID]abstract.TableInfo{
		tableID: {
			EtaRow: rows,
			IsView: false,
			Schema: s.tableSchema,
		},
	}, nil
}

func (s *Storage) ExactTableRowsCount(table abstract.TableID) (uint64, error) {
	return s.EstimateTableRowsCount(table)
}

func (s *Storage) EstimateTableRowsCount(table abstract.TableID) (uint64, error) {
	if s.cfg.EventSource.SQS != nil {
		// we are in a replication, possible millions/billions of files in bucket, estimating rows expensive
		return 0, nil
	}
	if rowCounter, ok := s.reader.(reader.RowsCountEstimator); ok {
		return rowCounter.EstimateRowsCountAllObjects(context.Background())
	}
	return 0, nil
}

func (s *Storage) TableExists(table abstract.TableID) (bool, error) {
	return table == *abstract.NewTableID(s.cfg.TableNamespace, s.cfg.TableName), nil
}

func (s *Storage) ShardingContext() ([]byte, error) {
	result, err := coordinator_utils.BuildShardingContext(
		context.Background(),
		s.logger,
		s.registry,
		s.cfg,
	)
	if err != nil {
		return nil, xerrors.Errorf("unable to get sharding context: %w", err)
	}

	return result, nil
}

func (s *Storage) SetShardingContext(shardingContext []byte) error {
	s.shardingContext = shardingContext
	return nil
}

func (s *Storage) BuildSharedMemory(
	ctx context.Context,
	transfer any,
	workerType abstract.WorkerType,
	cp any,
) (abstract.SharedMemory, error) {
	transferUnwrapped, ok := transfer.(*model.Transfer)
	if !ok {
		return nil, xerrors.Errorf("invalid transfer type: %T", transfer)
	}
	cpUnwrapped, ok := cp.(coordinator.Coordinator)
	if !ok {
		return nil, xerrors.Errorf("invalid transfer coordinator: %T", cp)
	}

	// RESET WORKERS DONE (in TransferState)
	if workerType == abstract.WorkerTypeMain {
		runtimeUnwrapped, ok := transferUnwrapped.Runtime.(abstract.ShardingTaskRuntime)
		if !ok {
			return nil, xerrors.Errorf("runtime is unsupported type, %T", transferUnwrapped.Runtime)
		}
		err := coordinator_utils.ResetWorkersDone(cpUnwrapped, transferUnwrapped.ID, runtimeUnwrapped)
		if err != nil {
			return nil, xerrors.Errorf("unable to reset workers, err: %w", err)
		}
	}

	result, err := s3_shared_memory.NewSharedMemory(
		ctx,
		s.logger,
		s.registry,
		transferUnwrapped,
		workerType,
		cpUnwrapped,
		s.shardingContext,
	)
	if err != nil {
		return nil, xerrors.Errorf("unable to build s3_shared_memory, err: %w", err)
	}
	return result, nil
}

func (s *Storage) CheckSecondaryWorkersDone(startTime time.Time, cp any, transfer any, operationID string) (bool, error) {
	cpUnwrapped, ok := cp.(coordinator.Coordinator)
	if !ok {
		return false, xerrors.Errorf("invalid transfer coordinator, err: %T", cp)
	}
	transferUnwrapped, ok := transfer.(*model.Transfer)
	if !ok {
		return false, xerrors.Errorf("invalid transfer type: %T", transfer)
	}
	runtimeUnwrapped, ok := transferUnwrapped.Runtime.(abstract.ShardingTaskRuntime)
	if !ok {
		return false, xerrors.Errorf("runtime is unsupported type, %T", transferUnwrapped.Runtime)
	}

	// check if any secondary worker have an error
	workers, err := cpUnwrapped.GetOperationWorkers(operationID)
	if err != nil {
		return false, errors.CategorizedErrorf(categories.Internal, "can't to get workers for operation '%v': %w", operationID, err)
	}
	errs := model.AggregateWorkerErrors(workers, operationID)
	if len(errs) > 0 {
		return false, xerrors.Errorf("errors detected on secondary workers: %v", errs)
	}

	// check is all workers successfully done
	maxEffectiveWorkersCount := effective_worker_num.DetermineMaxEffectiveWorkerNum(runtimeUnwrapped)
	count, err := coordinator_utils.WorkersDoneCount(startTime, cpUnwrapped, s.transferID, maxEffectiveWorkersCount)
	if err != nil {
		return false, xerrors.Errorf("unable to check workers, err: %w", err)
	}
	if count == maxEffectiveWorkersCount {
		s.logger.Infof("wait while secondary_workers done - done %d/%d (DONE)", count, maxEffectiveWorkersCount)
		return true, nil
	} else {
		s.logger.Infof("wait while secondary_workers done - done %d/%d (NOT_DONE)", count, maxEffectiveWorkersCount)
		return false, nil
	}
}

func New(src *s3.S3Source, transferID string, lgr log.Logger, registry metrics.Registry) (*Storage, error) {
	sess, err := s3.NewAWSSession(lgr, src.Bucket, src.ConnectionConfig)
	if err != nil {
		return nil, xerrors.Errorf("failed to create aws session: %w", err)
	}
	currReader, err := reader_factory.NewReader(src, lgr, sess, stats.NewSourceStats(registry))
	if err != nil {
		return nil, xerrors.Errorf("unable to create reader: %w", err)
	}
	tableSchema, err := currReader.ResolveSchema(context.Background())
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve schema: %w", err)
	}
	return &Storage{
		cfg:             src,
		transferID:      transferID,
		client:          aws_s3.New(sess),
		logger:          lgr,
		tableSchema:     tableSchema,
		reader:          currReader,
		registry:        registry,
		shardingContext: nil,
	}, nil
}
