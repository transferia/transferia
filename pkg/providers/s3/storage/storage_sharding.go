package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/predicate"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
)

// To verify providers contract implementation
var (
	_ abstract.ShardingStorage = (*Storage)(nil)
)

const (
	s3FileNameCol = "s3_file_name"
	s3VersionCol  = "s3_file_version"
)

func (s *Storage) ShardTable(ctx context.Context, tdsec abstract.TableDescription) ([]abstract.TableDescription, error) {
	s.logger.Infof("try to shard: %v", tdsec.String())

	files, err := reader.ListFiles(s.cfg.Bucket, s.cfg.PathPrefix, s.cfg.PathPattern, s.client, s.logger, nil, s.reader.ObjectsFilter())
	if err != nil {
		return nil, xerrors.Errorf("unable to load file list: %w", err)
	}

	rowCounter, ok := s.reader.(reader.RowCounter)
	if !ok {
		return nil, xerrors.NewSentinel("missing row counter for sharding rows estimation")
	}
	operands, err := predicate.InclusionOperands(tdsec.Filter, s3VersionCol)
	if err != nil {
		return nil, xerrors.Errorf("unable to extract: %s: filter: %w", s3VersionCol, err)
	}
	var res []abstract.TableDescription
	etaRows, err := rowCounter.TotalRowCount(ctx)
	if err != nil {
		return nil, xerrors.Errorf("unable to estimate row count: %w", err)
	}
	for _, file := range files {
		if !s.matchOperands(operands, file) {
			continue
		}
		var rows uint64
		if len(files) > reader.EstimateFilesLimit {
			rows = uint64(float64(etaRows) / float64(len(files)))
		} else {
			rows, err = rowCounter.RowCount(context.Background(), file)
			if err != nil {
				return nil, xerrors.Errorf("failed to fetch row count for file: %s : %w", *file.Key, err)
			}
		}
		// TODO: shard file on file byte ranges
		res = append(res, abstract.TableDescription{
			Name:   s.cfg.TableName,
			Schema: s.cfg.TableNamespace,
			Filter: abstract.FiltersIntersection(
				tdsec.Filter,
				abstract.WhereStatement(fmt.Sprintf(`"%s" = '%s'`, s3FileNameCol, *file.Key)),
			),
			EtaRow: rows,
			Offset: 0,
		})
	}
	return res, nil
}

func (s *Storage) matchOperands(operands []predicate.Operand, file *s3.Object) bool {
	if len(operands) == 0 {
		return true
	}
	versionStr := file.LastModified.UTC().Format(time.RFC3339)
	for _, op := range operands {
		if !op.Match(versionStr) {
			s.logger.Infof("skip file: %s due %s(%s) not match operand: %v", *file.Key, s3VersionCol, versionStr, op)
			return false
		}
	}
	return true
}
