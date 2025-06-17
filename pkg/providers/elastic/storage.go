//go:build !disable_elastic_provider

package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/jsonx"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	chunkSize               = 5 * 1000
	chunkByteSize           = 128 * 1024 * 1024
	maxResultsInSingleFetch = 10000 // elasticsearch limit is 10 000
	scrollDuration          = time.Minute * 60
)

type Storage struct {
	Cfg     *elasticsearch.Config
	Client  *elasticsearch.Client
	Metrics *stats.SourceStats
	IsHomo  bool
}

func (s *Storage) Close() {
}

func (s *Storage) Ping() error {
	res, err := s.Client.API.Ping()
	if err != nil {
		return xerrors.Errorf("unable to ping cluster: %w", err)
	}
	if res.IsError() {
		return xerrors.Errorf("error pinging cluster, HTTP status: %s, err: %s", res.Status(), res.String())
	}
	return nil
}

func (s *Storage) EstimateTableRowsCount(table abstract.TableID) (uint64, error) {
	return s.ExactTableRowsCount(table)
}

func (s *Storage) ExactTableRowsCount(table abstract.TableID) (uint64, error) {
	indexName, err := makeIndexNameFromTableID(table)
	if err != nil {
		return 0, xerrors.Errorf("can't make index name from %v: %w", table.String(), err)
	}

	body, err := getResponseBody(s.Client.Count(s.Client.Count.WithIndex(indexName)))
	if err != nil {
		return 0, xerrors.Errorf("unable to count rows, index: %s, err: %w", indexName, err)
	}

	var counted countResponse
	if err := jsonx.Unmarshal(body, &counted); err != nil {
		return 0, xerrors.Errorf("failed to unmarshal counted rows, index: %s, err: %w", indexName, err)
	}

	return counted.Count, nil
}

func (s *Storage) TableSchema(_ context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
	schema, err := s.getSchema(table.Name)
	if err != nil {
		return nil, xerrors.Errorf("unable to get schema: %s: %w", table.Name, err)
	}
	return abstract.NewTableSchema(schema.Columns), nil
}

func (s *Storage) LoadTable(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	st := util.GetTimestampFromContextOrNow(ctx)

	exist, err := s.TableExists(table.ID())
	if err != nil || !exist {
		return xerrors.Errorf("could not find table to load: %s, err: %w", table.Name, err)
	}

	filter, err := filterFromTable(table)
	if err != nil {
		return xerrors.Errorf("could not extract filter from table description: %s, err: %w", table.Name, err)
	}

	var body []byte
	if filter.Max == 0 {
		// no sharding possible
		body, err = getResponseBody(s.Client.Search(
			s.Client.Search.WithIndex(table.Name),
			s.Client.Search.WithScroll(scrollDuration),
			s.Client.Search.WithSize(maxResultsInSingleFetch)))
	} else {
		body, err = getResponseBody(s.Client.Search(
			s.Client.Search.WithIndex(table.Name),
			s.Client.Search.WithScroll(scrollDuration),
			s.Client.Search.WithSize(maxResultsInSingleFetch),
			s.Client.Search.WithBody(strings.NewReader(fmt.Sprintf(`{
			"slice": {
				"id": %d,
				"max": %d
			}
		}`, filter.ID, filter.Max)))))
	}

	if err != nil {
		return xerrors.Errorf("unable to fetch docs, index: %s, err: %w", table.Name, err)
	}

	var result searchResponse
	if err := jsonx.Unmarshal(body, &result); err != nil {
		return xerrors.Errorf("failed to unmarshal docs, index: %s, err: %w", table.Name, err)
	}

	err = s.readRowsAndPushByChunks(
		&result,
		st,
		table,
		chunkSize,
		chunkByteSize,
		pusher,
	)
	if err != nil {
		return err
	}

	return nil
}

func (s *Storage) TableExists(table abstract.TableID) (bool, error) {
	indexName, err := makeIndexNameFromTableID(table)
	if err != nil {
		return false, xerrors.Errorf("can't make index name from %v: %w", table.String(), err)
	}
	res, err := s.Client.Indices.Exists([]string{indexName})
	if err != nil {
		return false, xerrors.Errorf("unable to verify index existence, index: %s, err: %w", indexName, err)
	}
	if res.IsError() {
		return false, xerrors.Errorf("error verifying index existence, index: %s, HTTP status: %s, err: %s", indexName, res.Status(), res.String())
	}

	return true, nil
}

func (s *Storage) TableList(includeTableFilter abstract.IncludeTableList) (abstract.TableMap, error) {
	body, err := getResponseBody(s.Client.Indices.Stats())
	if err != nil {
		return nil, xerrors.Errorf("unable to fetch elastic stats: %w", err)
	}
	var stats statsResponse
	if err := jsonx.Unmarshal(body, &stats); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal elastic stats: %w", err)
	}

	tables := make(abstract.TableMap)

	for index := range stats.Indices {
		if strings.HasPrefix(index, ".") {
			// skip internal indices like .geoip_databases for example
			continue
		}
		schema, err := s.getSchema(index)
		if err != nil {
			return nil, xerrors.Errorf("failed to fetch schema, index %s : %w", index, err)
		}

		etaRow, err := s.EstimateTableRowsCount(abstract.TableID{
			Name:      index,
			Namespace: "",
		})
		if err != nil {
			return nil, xerrors.Errorf("failed to fetch estimated rows count, index %s : %w", index, err)
		}

		tableID := abstract.TableID{Namespace: "", Name: index}
		tables[tableID] = abstract.TableInfo{
			EtaRow: uint64(etaRow),
			IsView: false,
			Schema: abstract.NewTableSchema(schema.Columns),
		}
	}

	return model.FilteredMap(tables, includeTableFilter), nil
}

func (s *Storage) getSchema(index string) (*SchemaDescription, error) {
	body, err := getResponseBody(s.Client.Indices.GetMapping(s.Client.Indices.GetMapping.WithIndex(index)))
	if err != nil {
		return nil, xerrors.Errorf("unable to fetch mappings: %w", err)
	}

	var mappings map[string]mapping
	if err := jsonx.Unmarshal(body, &mappings); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal mappings: %w", err)
	}

	indexMapping, ok := mappings[index]
	if !ok {
		return nil, xerrors.Errorf("failed to find mapping, index: %s", index)
	}

	schema, err := s.getSchemaFromElasticMapping(indexMapping.Mappings, s.IsHomo)
	if err != nil {
		return nil, xerrors.Errorf("failed to get schema from elastic mapping: %w", err)
	}

	// fix data types - useless for homo-like delivery, moreover it can lead to OOMs & errors like TM-7691
	if !s.IsHomo {
		if err := s.fixDataTypesWithSampleData(index, schema); err != nil {
			return nil, xerrors.Errorf("failed to amend schema based on sample data: %w", err)
		}
	}

	return schema, nil
}

func (s *Storage) getRawIndexParams(index string) ([]byte, error) {
	body, err := getResponseBody(s.Client.Indices.Get([]string{index}))
	if err != nil {
		return nil, xerrors.Errorf("unable to fetch index params: %w", err)
	}

	var indexesParams map[string]interface{}
	if err := jsonx.Unmarshal(body, &indexesParams); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal index params: %w", err)
	}
	indexParams, ok := indexesParams[index]
	if !ok {
		return nil, xerrors.Errorf("failed to find index params for: %s", index)
	}
	DeleteSystemFieldsFromIndexParams(indexParams.(map[string]interface{}))

	return json.Marshal(indexParams)
}

type StorageOpt func(storage *Storage) *Storage

func WithHomo() StorageOpt {
	return func(storage *Storage) *Storage {
		storage.IsHomo = true
		return storage
	}
}

func WithOpts(storage *Storage, opts ...StorageOpt) *Storage {
	for _, opt := range opts {
		storage = opt(storage)
	}
	return storage
}

func NewStorage(src *ElasticSearchSource, logger log.Logger, mRegistry metrics.Registry, serverType ServerType, opts ...StorageOpt) (*Storage, error) {
	config, err := ConfigFromDestination(logger, src.SourceToElasticSearchDestination(), serverType)
	if err != nil {
		return nil, xerrors.Errorf("failed to create elastic configuration: %w", err)
	}
	client, err := WithLogger(*config, log.With(logger, log.Any("component", "esclient")), serverType)
	if err != nil {
		return nil, xerrors.Errorf("failed to create elastic client: %w", err)
	}

	return WithOpts(&Storage{
		Cfg:     config,
		Client:  client,
		Metrics: stats.NewSourceStats(mRegistry),
		IsHomo:  false,
	}, opts...), nil
}
