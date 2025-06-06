package storage

import (
	"context"
	"fmt"
	"strings"
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

type FileWithStats struct {
	*s3.Object
	Rows, Size uint64
}

// NOTE: calculateFilesStats stores 0 as result's `Size` fields if `needSizes` is false,
// otherwise it could go to S3 API and elapsed time will increase.
func (s *Storage) calculateFilesStats(ctx context.Context, files []*s3.Object, needSizes bool) ([]*FileWithStats, error) {
	rowCounter, ok := s.reader.(reader.RowsCountEstimator)
	if !ok {
		return nil, xerrors.NewSentinel("missing row counter for sharding rows estimation")
	}
	etaRows, err := rowCounter.EstimateRowsCountAllObjects(ctx)
	if err != nil {
		return nil, xerrors.Errorf("unable to estimate row count: %w", err)
	}
	res := make([]*FileWithStats, 0, len(files))
	for _, file := range files {
		rows := etaRows / uint64(len(files)) // By default, use average value as rows count.
		if len(files) <= reader.EstimateFilesLimit {
			// If number of files is few, count rows exactly.
			if rows, err = rowCounter.EstimateRowsCountOneObject(ctx, file); err != nil {
				return nil, xerrors.Errorf("unable to fetch row count for file '%s': %w", *file.Key, err)
			}
		}
		size := uint64(0)
		if needSizes {
			if size, err = reader.FileSize(s.cfg.Bucket, file, s.client, s.logger); err != nil {
				return nil, xerrors.Errorf("unable to get file size: %w", err)
			}
		}
		res = append(res, &FileWithStats{Object: file, Rows: rows, Size: size})
	}
	return res, nil
}

func (s *Storage) ShardTable(ctx context.Context, tdesc abstract.TableDescription) ([]abstract.TableDescription, error) {
	s.logger.Infof("try to shard: %v", tdesc.String())
	operands, err := predicate.InclusionOperands(tdesc.Filter, s3VersionCol)
	if err != nil {
		return nil, xerrors.Errorf("unable to extract '%s' filter: %w", s3VersionCol, err)
	}
	filesFilter := reader.ObjectsFilter(func(file *s3.Object) bool {
		if !reader.IsNotEmpty(file) {
			return false // Skip empty files.
		}
		return s.matchOperands(operands, file)
	})
	listedFiles, err := reader.ListFiles(s.cfg.Bucket, s.cfg.PathPrefix, s.cfg.PathPattern, s.client, s.logger, nil, filesFilter)
	if err != nil {
		return nil, xerrors.Errorf("unable to load file list: %w", err)
	}

	needSizes := s.cfg.ShardingParams != nil // Calculate sizes of files only if custom sharding enabled.
	files, err := s.calculateFilesStats(ctx, listedFiles, needSizes)
	if err != nil {
		return nil, xerrors.Errorf("unable to get files stats: %w", err)
	}

	if s.cfg.ShardingParams != nil {
		return s.shardByLimits(files)
	}
	return s.shardDefault(tdesc, files), nil
}

func (s *Storage) shardDefault(tdesc abstract.TableDescription, files []*FileWithStats) []abstract.TableDescription {
	res := make([]abstract.TableDescription, 0, len(files))
	for _, file := range files {
		res = append(res, abstract.TableDescription{
			Name:   s.cfg.TableName,
			Schema: s.cfg.TableNamespace,
			Filter: abstract.FiltersIntersection(
				tdesc.Filter,
				abstract.WhereStatement(fmt.Sprintf(`"%s" = '%s'`, s3FileNameCol, *file.Key)),
			),
			EtaRow: file.Rows,
			Offset: 0,
		})
	}
	return res
}

func (s *Storage) shardByLimits(files []*FileWithStats) ([]abstract.TableDescription, error) {
	if s.cfg.ShardingParams == nil {
		return nil, xerrors.New("sharding params is not set")
	}
	params := s.cfg.ShardingParams

	var res []abstract.TableDescription
	var partFiles []string
	var partSize, partRows uint64
	for i, file := range files {
		partFiles = append(partFiles, *file.Key)
		partSize += file.Size
		partRows += file.Rows

		isLast := (i == len(files)-1)
		if isLast || partSize >= params.PartBytesLimit || uint64(len(partFiles)) >= params.PartFilesLimit {
			res = append(res, abstract.TableDescription{
				Name:   s.cfg.TableName,
				Schema: s.cfg.TableNamespace,
				Filter: abstract.WhereStatement(fmt.Sprintf("%s IN ('%s')", s3FileNameCol, strings.Join(partFiles, "','"))),
				EtaRow: partRows,
				Offset: 0,
			})
			partFiles, partSize, partRows = nil, 0, 0
		}
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
