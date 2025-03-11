package storage

import (
	"context"

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferria/transferria/library/go/core/metrics"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/predicate"
	"github.com/transferria/transferria/pkg/providers/s3"
	"github.com/transferria/transferria/pkg/providers/s3/pusher"
	"github.com/transferria/transferria/pkg/providers/s3/reader"
	"github.com/transferria/transferria/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ abstract.Storage = (*Storage)(nil)

type Storage struct {
	cfg         *s3.S3Source
	client      s3iface.S3API
	logger      log.Logger
	tableSchema *abstract.TableSchema
	reader      reader.Reader
	registry    metrics.Registry
}

func (s *Storage) Close() {
}

func (s *Storage) Ping() error {
	return nil
}

func (s *Storage) TableSchema(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
	return s.tableSchema, nil
}

func (s *Storage) LoadTable(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	fileOps, err := predicate.InclusionOperands(table.Filter, s3FileNameCol)
	if err != nil {
		return xerrors.Errorf("unable to extract: %s: filter: %w", s3FileNameCol, err)
	}
	if len(fileOps) > 0 {
		return s.readFile(ctx, table, pusher)
	}
	parts, err := s.ShardTable(ctx, table)
	if err != nil {
		return xerrors.Errorf("unable to load files to read: %w", err)
	}
	totalRows := uint64(0)
	for _, part := range parts {
		totalRows += part.EtaRow
	}
	for _, part := range parts {
		if err := s.readFile(ctx, part, pusher); err != nil {
			return xerrors.Errorf("unable to read part: %v: %w", part.String(), err)
		}
	}
	return nil
}

func (s *Storage) readFile(ctx context.Context, part abstract.TableDescription, syncPusher abstract.Pusher) error {
	fileOps, err := predicate.InclusionOperands(part.Filter, s3FileNameCol)
	if err != nil {
		return xerrors.Errorf("unable to extract: %s: filter: %w", s3FileNameCol, err)
	}
	if len(fileOps) != 1 {
		return xerrors.Errorf("expect single col in filter: %s, but got: %v", part.Filter, len(fileOps))
	}
	fileOp := fileOps[0]
	if fileOp.Op != predicate.EQ {
		return xerrors.Errorf("file predicate expected to be `=`, but got: %s", fileOp)
	}
	fileName, ok := fileOp.Val.(string)
	if !ok {
		return xerrors.Errorf("%s expected to be string, but got: %T", s3FileNameCol, fileOp.Val)
	}
	pusher := pusher.New(syncPusher, nil, s.logger, 0)
	if err := s.reader.Read(ctx, fileName, pusher); err != nil {
		return xerrors.Errorf("unable to read file: %s: %w", part.Filter, err)
	}
	return nil
}

func (s *Storage) TableList(_ abstract.IncludeTableList) (abstract.TableMap, error) {
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
	if rowCounter, ok := s.reader.(reader.RowCounter); ok {
		return rowCounter.TotalRowCount(context.Background())
	}
	return 0, nil
}

func (s *Storage) TableExists(table abstract.TableID) (bool, error) {
	return table == *abstract.NewTableID(s.cfg.TableNamespace, s.cfg.TableName), nil
}

func New(src *s3.S3Source, lgr log.Logger, registry metrics.Registry) (*Storage, error) {
	sess, err := s3.NewAWSSession(lgr, src.Bucket, src.ConnectionConfig)
	if err != nil {
		return nil, xerrors.Errorf("failed to create aws session: %w", err)
	}

	r, err := reader.New(src, lgr, sess, stats.NewSourceStats(registry))
	if err != nil {
		return nil, xerrors.Errorf("unable to create reader: %w", err)
	}
	tableSchema, err := r.ResolveSchema(context.Background())
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve schema: %w", err)
	}
	return &Storage{
		cfg:         src,
		client:      aws_s3.New(sess),
		logger:      lgr,
		tableSchema: tableSchema,
		reader:      r,
		registry:    registry,
	}, nil
}
