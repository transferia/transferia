package sample

import (
	"context"
	"time"

	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ abstract.Storage = (*Storage)(nil)

type Storage struct {
	ctx                context.Context
	metrics            *stats.SourceStats
	cancel             func()
	SampleType         string
	SnapshotEventCount int64
	TableName          string
	MaxSampleData      int64
	MinSleepTime       time.Duration
	logger             log.Logger
}

func (s *Storage) Close() {
	s.cancel()
}

func (s *Storage) Ping() error {
	return nil
}

func (s *Storage) LoadTable(ctx context.Context, _ abstract.TableDescription, pusher abstract.Pusher) error {
	rowsCount := int64(0)
	s.logger.Infof("The total row number is %v", s.SnapshotEventCount)

	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("Stopping run")
			return nil
		default:
		}

		sampleData := s.MaxSampleData
		if rowsCount+sampleData > s.SnapshotEventCount {
			sampleData = s.SnapshotEventCount - rowsCount
		}
		data := generateRandomDataForSampleType(s.SampleType, s.MaxSampleData, s.TableName, s.metrics)
		rowsCount += sampleData
		s.metrics.ChangeItems.Add(int64(len(data)))
		if err := pusher(data); err != nil {
			s.logger.Errorf("unable to push %v rows to %v: %v", sampleData, s.TableName, err)
			return xerrors.Errorf("unable to push last %d rows to table %s: %w", sampleData, s.TableName, err)
		}
		if rowsCount >= s.SnapshotEventCount {
			s.logger.Infof("Reached the target of %d events", s.SnapshotEventCount)
			break
		}
		s.logger.Infof("Current row count is %v", rowsCount)
	}

	return nil
}

func (s *Storage) TableSchema(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
	if s.SampleType == userActivitiesSampleType {
		return userActivitiesColumnSchema, nil
	}
	return iotDataColumnSchema, nil
}

func (s *Storage) TableList(filter abstract.IncludeTableList) (abstract.TableMap, error) {
	tables := make(abstract.TableMap)
	tableID := abstract.TableID{Namespace: "", Name: s.TableName}

	if s.SampleType == userActivitiesSampleType {
		tables[tableID] = abstract.TableInfo{
			EtaRow: uint64(s.SnapshotEventCount),
			IsView: false,
			Schema: userActivitiesColumnSchema,
		}
		return tables, nil
	}

	tables[tableID] = abstract.TableInfo{
		EtaRow: uint64(s.SnapshotEventCount),
		IsView: false,
		Schema: iotDataColumnSchema,
	}

	return tables, nil
}

func (s *Storage) ExactTableRowsCount(_ abstract.TableID) (uint64, error) {
	return uint64(s.SnapshotEventCount), nil
}

func (s *Storage) EstimateTableRowsCount(_ abstract.TableID) (uint64, error) {
	return uint64(s.SnapshotEventCount), nil
}

func (s *Storage) TableExists(_ abstract.TableID) (bool, error) {
	return true, nil
}

func NewStorage(config *SampleSource, log log.Logger) (*Storage, error) {
	ctx, cancel := context.WithCancel(context.Background())
	return &Storage{
		ctx:                ctx,
		metrics:            stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
		SampleType:         config.SampleType,
		SnapshotEventCount: config.SnapshotEventCount,
		TableName:          config.TableName,
		MaxSampleData:      config.MaxSampleData,
		MinSleepTime:       config.MinSleepTime,
		logger:             log,
		cancel:             cancel,
	}, nil
}
