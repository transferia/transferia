//go:build !disable_s3_provider

package storage

import (
	"context"

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/yandex/cloud/filter"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/predicate"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/pusher"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/stats"
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

func (s *Storage) LoadTable(ctx context.Context, table abstract.TableDescription, inPusher abstract.Pusher) error {
	if s.cfg.ShardingParams != nil { // TODO: Remove that `if` in TM-8537.
		// @booec branch

		// With enabled sharding params, common-known cloud filter parser is used.
		// Unfortunatelly, for default sharding (when ShardingParams == nil) self-written pkg/predicate is used.
		// Since there are no purposes to use self-written filter parser, it should be refactored in TM-8537.
		syncPusher := pusher.New(func(items []abstract.ChangeItem) error {
			for i, item := range items {
				if item.IsRowEvent() {
					items[i].PartID = table.PartID()
					items[i].Schema = s.cfg.TableNamespace
					items[i].Table = s.cfg.TableName
				}
			}
			return inPusher(items)
		}, nil, s.logger, 0)
		if err := s.readFiles(ctx, table, syncPusher); err != nil {
			return xerrors.Errorf("unable to read many files: %w", err)
		}
		return nil
	} else {
		// @tserakhau branch

		fileOps, err := predicate.InclusionOperands(table.Filter, s3FileNameCol)
		if err != nil {
			return xerrors.Errorf("unable to extract: %s: filter: %w", s3FileNameCol, err)
		}
		if len(fileOps) > 0 {
			return s.readFile(ctx, table, inPusher)
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
			if err := s.readFile(ctx, part, inPusher); err != nil {
				return xerrors.Errorf("unable to read part: %v: %w", part.String(), err)
			}
		}
		return nil
	}
}

// readFiles read files extracted from IN-operator of part.Filter.
// For now, readFiles is used only with s.cfg.ShardingParams != nil and should be fixed in TM-8537.
func (s *Storage) readFiles(ctx context.Context, part abstract.TableDescription, syncPusher pusher.Pusher) error {
	terms, err := filter.Parse(string(part.Filter))
	if err != nil {
		return xerrors.Errorf("unable to parse filter: %w", err)
	}
	if len(terms) != 1 {
		return xerrors.Errorf("expected filter with only one 'IN' operator, got '%s'", part.Filter)
	}
	term := terms[0]
	if term.Operator != filter.In {
		return xerrors.Errorf("unexpected operator '%s' in filter '%s'", term.Operator.String(), part.Filter)
	}
	if term.Attribute != s3FileNameCol {
		return xerrors.Errorf("expected attr '%s', got '%s' in filter '%s'", s3FileNameCol, term.Attribute, part.Filter)
	}
	if !term.Value.IsStringList() {
		return xerrors.Errorf("expected []string value, got '%s' in filter '%s'", term.Value.Type(), part.Filter)
	}
	for _, filePath := range term.Value.AsStringList() {
		if err := s.reader.Read(ctx, filePath, syncPusher); err != nil {
			return xerrors.Errorf("unable to read file %s: %w", filePath, err)
		}
	}
	return nil
}

func (s *Storage) readFile(ctx context.Context, part abstract.TableDescription, inPusher abstract.Pusher) error {
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
	syncPusher := pusher.New(inPusher, nil, s.logger, 0)
	if err := s.reader.Read(ctx, fileName, syncPusher); err != nil {
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
	if rowCounter, ok := s.reader.(reader.RowsCountEstimator); ok {
		return rowCounter.EstimateRowsCountAllObjects(context.Background())
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

	currReader, err := reader.New(src, lgr, sess, stats.NewSourceStats(registry))
	if err != nil {
		return nil, xerrors.Errorf("unable to create reader: %w", err)
	}
	tableSchema, err := currReader.ResolveSchema(context.Background())
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve schema: %w", err)
	}

	return &Storage{
		cfg:         src,
		client:      aws_s3.New(sess),
		logger:      lgr,
		tableSchema: tableSchema,
		reader:      currReader,
		registry:    registry,
	}, nil
}
