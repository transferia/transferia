//go:build !disable_clickhouse_provider

package clickhouse

import (
	"context"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/base/filter"
	"github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"go.ytsaurus.tech/library/go/core/log"
)

type ClusterTables struct {
	tables     []*Table
	iter       int
	storage    ClickhouseStorage
	partFilter map[string]bool
	shards     map[string][]*clickhouse.Host
	rowFilter  map[abstract.TableID]string
}

func (s *ClusterTables) ParsePartKey(data string) (*abstract.TableID, error) {
	for _, table := range s.tables {
		for _, part := range table.parts {
			if part.Key() == data {
				return &part.Table, nil
			}
		}
	}
	return nil, xerrors.Errorf("table part: %s not found", data)
}

func (s *ClusterTables) Next() bool {
	s.iter++
	return s.iter < len(s.tables)
}

func (s *ClusterTables) Err() error {
	return nil
}

func (s *ClusterTables) Close() {
}

func (s *ClusterTables) Object() (base.DataObject, error) {
	t := s.tables[s.iter]
	if len(s.partFilter) > 0 {
		t.partFilter = s.partFilter
	}
	if tableF, ok := s.rowFilter[t.tableID]; ok && tableF != "" {
		t.rowFilter = abstract.WhereStatement(tableF)
	}
	return t, nil
}

func (s *ClusterTables) ToOldTableMap() (abstract.TableMap, error) {
	return nil, xerrors.New("not supported")
}

func (s *ClusterTables) AddTable(tid abstract.TableID) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	parts, err := s.storage.TableParts(ctx, tid)
	if err != nil {
		return xerrors.Errorf("unable to resolve table parts: %w", err)
	}
	s.tables = append(s.tables, NewTable(tid, parts, s.shards, ""))
	return nil
}

func (s *ClusterTables) AddTableDescription(desc abstract.TableDescription) error {
	var parts []TablePart
	for s := range s.shards {
		parts = append(parts, TablePart{
			Table:      desc.ID(),
			Name:       "",
			Rows:       int64(desc.EtaRow),
			Bytes:      0,
			Shard:      s,
			ShardCount: 0,
		})
	}
	if len(s.shards) == 0 {
		parts = append(parts, TablePart{
			Table:      desc.ID(),
			Name:       "",
			Rows:       int64(desc.EtaRow),
			Bytes:      0,
			Shard:      "",
			ShardCount: 0,
		})
	}
	s.tables = append(s.tables, NewTable(desc.ID(), parts, s.shards, desc.Filter))
	return nil
}

func newClusterTablesFromDescription(storage ClickhouseStorage, config *model.ChSource, descriptions []abstract.TableDescription) (*ClusterTables, error) {
	sinkParams, err := config.ToSinkParams()
	if err != nil {
		return nil, xerrors.Errorf("unable to get sink params: %w", err)
	}

	objs := &ClusterTables{
		tables:     make([]*Table, 0),
		iter:       -1,
		storage:    storage,
		partFilter: map[string]bool{},
		shards:     sinkParams.Shards(),
		rowFilter:  map[abstract.TableID]string{},
	}
	for _, desc := range descriptions {
		if err := objs.AddTableDescription(desc); err != nil {
			return nil, xerrors.Errorf("unable to add table description: %w", err)
		}
	}
	return objs, nil
}

// Hack: convert base.DataObjectFilter to abstract.IncludeTableList to CH storage
type filterWrapper struct {
	baseFilter base.DataObjectFilter
}

func (f *filterWrapper) Include(tID abstract.TableID) bool {
	if f.baseFilter == nil {
		return true
	}
	res, err := f.baseFilter.IncludesID(tID)
	if err != nil {
		logger.Log.Warn("Error checking table include for table", log.String("table", tID.String()), log.Error(err))
		return false
	}
	return res
}

func (f *filterWrapper) IncludeTableList() ([]abstract.TableID, error) {
	// Fortunately CH storage doesn't use this method
	return nil, xerrors.New("table listing is not supported by this filter")
}

func NewClusterTables(storage ClickhouseStorage, config *model.ChSource, inputFilter base.DataObjectFilter) (*ClusterTables, error) {
	if descriptionFilter, ok := inputFilter.(filter.FilterableFilter); ok {
		tableDescriptions, err := descriptionFilter.ListFilters()
		if err != nil {
			return nil, xerrors.Errorf("unable to list filters: %w", err)
		}
		tableDescriptions = yslices.Filter(tableDescriptions, func(description abstract.TableDescription) bool {
			ok, err := inputFilter.IncludesID(description.ID())
			return ok && err == nil
		})
		if len(tableDescriptions) > 0 {
			return newClusterTablesFromDescription(storage, config, tableDescriptions)
		}
	}

	tables, err := storage.TableList(&filterWrapper{inputFilter})
	if err != nil {
		return nil, xerrors.Errorf("unable to list tables: %w", err)
	}
	sinkParams, err := config.ToSinkParams()
	if err != nil {
		return nil, xerrors.Errorf("unable to get sink params: %w", err)
	}
	objs := &ClusterTables{
		tables:     make([]*Table, 0),
		iter:       -1,
		storage:    storage,
		partFilter: map[string]bool{},
		shards:     sinkParams.Shards(),
		rowFilter:  map[abstract.TableID]string{},
	}

	for tid := range tables {
		if err := objs.AddTable(tid); err != nil {
			return nil, xerrors.Errorf("unable to add table parts: %v: %w", tid.Fqtn(), err)
		}
	}
	return objs, nil
}
