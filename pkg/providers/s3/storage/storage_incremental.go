//go:build !disable_s3_provider

package storage

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/araddon/dateparse"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
)

// To verify providers contract implementation
var (
	_ abstract.IncrementalStorage = (*Storage)(nil)
)

func (s *Storage) GetNextIncrementalState(ctx context.Context, incremental []abstract.IncrementalTable) ([]abstract.IncrementalState, error) {
	if len(incremental) == 0 {
		return nil, nil // incremental mode is not configured
	}
	if len(incremental) > 1 {
		return nil, abstract.NewFatalError(xerrors.Errorf("s3 source provide single table: %s.%s, but incremental configure %d tables", s.cfg.TableNamespace, s.cfg.TableName, len(incremental)))
	}
	tbl := incremental[0]
	if tbl.TableID() != s.cfg.TableID() {
		return nil, xerrors.Errorf("table ID not matched, expected: %v, got: %v", s.cfg.TableID(), tbl.TableID())
	}
	tDesc := abstract.IncrementalState{
		Name:    tbl.Name,
		Schema:  tbl.Namespace,
		Payload: "",
	}
	if tbl.InitialState != "" {
		var versonTS time.Time
		minDate, err := dateparse.ParseAny(tbl.InitialState)
		if err != nil {
			return nil, xerrors.Errorf("unable to parse initial state: %s, must be valid date: %w", tbl.InitialState, err)
		}
		versonTS = minDate
		tDesc.Payload = abstract.FiltersIntersection(tDesc.Payload, abstract.WhereStatement(fmt.Sprintf(`"%s" > '%s'`, s3VersionCol, versonTS.UTC().Format(time.RFC3339))))
		return []abstract.IncrementalState{tDesc}, nil
	} else {
		var newest time.Time
		s.logger.Infof("no initial value, try to find newest file")

		var currentMarker *string
		endOfBucket := false
		for {
			if err := s.client.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
				Bucket:  aws.String(s.cfg.Bucket),
				Prefix:  aws.String(s.cfg.PathPrefix),
				MaxKeys: aws.Int64(1000),
				Marker:  currentMarker,
			}, func(o *s3.ListObjectsOutput, b bool) bool {
				for _, file := range o.Contents {
					currentMarker = file.Key
					s.logger.Infof("file %s: %s", *file.Key, *file.LastModified)
					if reader.SkipObject(file, s.cfg.PathPattern, "|", s.reader.ObjectsFilter()) {
						s.logger.Infof("file did not pass type/path check, skipping: file %s, pathPattern: %s", *file.Key, s.cfg.PathPattern)
						continue
					}
					if file.LastModified.Sub(newest) > 0 {
						newest = *file.LastModified
						continue
					}
				}
				if len(o.Contents) < 1000 {
					endOfBucket = true
				}
				return true
			}); err != nil {
				return nil, xerrors.Errorf("unable to list all objects: %w", err)
			}
			if endOfBucket {
				break
			}
		}

		includeTS := newest.UTC().Format(time.RFC3339)
		s.logger.Infof("found newest file %s: %s", s3VersionCol, includeTS)
		tDesc.Payload = abstract.FiltersIntersection(tDesc.Payload, abstract.WhereStatement(fmt.Sprintf(`"%s" > '%s'`, s3VersionCol, includeTS)))
		return []abstract.IncrementalState{tDesc}, nil
	}
}

func (s *Storage) BuildArrTableDescriptionWithIncrementalState(tables []abstract.TableDescription, incremental []abstract.IncrementalTable) []abstract.TableDescription {
	result := slices.Clone(tables)
	for i, table := range result {
		if table.Filter != "" || table.Offset != 0 {
			// table already contains predicate
			continue
		}
		for _, tbl := range incremental {
			if !tbl.Initialized() {
				continue
			}
			if table.ID() == tbl.TableID() {
				result[i] = abstract.TableDescription{
					Name:   tbl.Name,
					Schema: tbl.Namespace,
					Filter: abstract.WhereStatement(fmt.Sprintf(`"%s" < '%s'`, s3VersionCol, tbl.InitialState)),
					EtaRow: 0,
					Offset: 0,
				}
			}
		}
	}
	return result
}
