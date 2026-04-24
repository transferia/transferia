package elastic

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/errors/coded"
	error_codes "github.com/transferia/transferia/pkg/errors/codes"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
)

type Sink struct {
	cfg    *ElasticSearchDestination
	client *elasticsearch.Client
	logger log.Logger
	stats  *stats.SinkerStats // TODO - use it

	existsIndexes       *set.Set[abstract.TableID]
	indexNameToIsStream map[string]bool
}

func (s *Sink) Push(input []abstract.ChangeItem) error {
	lastCleanupChangeItemIndex := -1
	for i, changeItem := range input {
		if err := validateChangeItem(changeItem); err != nil {
			return abstract.NewFatalError(xerrors.Errorf("can't process changes: %w", err))
		}
		if changeItem.Kind == abstract.ElasticsearchDumpIndexKind {
			if err := s.applyIndexDump(changeItem); err != nil {
				return xerrors.Errorf("unable to prepare index: %w", err)
			}
		}

		if changeItem.Kind == abstract.DropTableKind {
			if err := s.pushBatch(input[lastCleanupChangeItemIndex+1 : i]); err != nil {
				return xerrors.Errorf("unable to push items: %w", err)
			}
			if err := s.dropIndex(changeItem.TableID()); err != nil {
				return xerrors.Errorf("can't drop index: %w", err)
			}
			lastCleanupChangeItemIndex = i
		}
	}
	return s.pushBatch(input[lastCleanupChangeItemIndex+1:])
}

func (s *Sink) applyIndexDump(item abstract.ChangeItem) error {
	if item.Kind != abstract.ElasticsearchDumpIndexKind {
		return nil
	}
	tableID := item.TableID()
	if s.existsIndexes.Contains(tableID) {
		return nil
	}

	indexName, _ := makeIndexNameFromTableID(tableID)

	response, err := s.client.Indices.Exists([]string{indexName})
	if err != nil {
		// classify SSL/transport errors during existence check
		if isSSLError(err) {
			return coded.Errorf(error_codes.OpenSearchSSLRequired, "ssl/transport error on exists(%q): %v", indexName, err)
		}
		return xerrors.Errorf("unable to check if index %q exists: %w", indexName, err)
	}
	if response.StatusCode == http.StatusOK {
		s.existsIndexes.Add(tableID)
		return nil
	}
	if response.StatusCode != http.StatusNotFound {
		// try to detect SSL required by response text
		if containsSSLRequired(response.String()) {
			return coded.Errorf(error_codes.OpenSearchSSLRequired, "ssl required when checking index %q: %s", indexName, response.String())
		}
		return xerrors.Errorf("wrong status code when checking index %q: %s", indexName, response.String())
	}

	dumpParams, ok := item.ColumnValues[0].(string)
	if !ok {
		return xerrors.Errorf("unable to extract the index dump data: %v, %T", item.ColumnValues[0], item.ColumnValues[0])
	}

	res, err := s.client.Indices.Create(indexName,
		s.client.Indices.Create.WithMasterTimeout(time.Second*30),
		s.client.Indices.Create.WithBody(strings.NewReader(dumpParams)),
	)
	if err != nil {
		if isSSLError(err) {
			return coded.Errorf(error_codes.OpenSearchSSLRequired, "ssl/transport error on create(%q): %v", indexName, err)
		}
		return xerrors.Errorf("unable to create the index %q: %w", indexName, err)
	}
	if res.IsError() {
		if containsSSLRequired(res.String()) {
			return coded.Errorf(error_codes.OpenSearchSSLRequired, "ssl required on create(%q): %s", indexName, res.String())
		}
		return xerrors.Errorf("error on creating the index %q: %s", indexName, res.String())
	}

	// wait until the index creation is applied
	err = WaitForIndexToExist(s.client, indexName, time.Second*30)
	if err != nil {
		return xerrors.Errorf("elastic check index creating error: %w", err)
	}

	s.existsIndexes.Add(tableID)
	return nil
}

func (s *Sink) dropIndex(tableID abstract.TableID) error {
	indexName, err := makeIndexNameFromTableID(tableID)
	if err != nil {
		return xerrors.Errorf("can't make index name from %v: %w", tableID.String(), err)
	}
	res, err := s.client.Indices.Delete([]string{indexName})
	if err != nil {
		return xerrors.Errorf("unable to delete index, index: %s, err: %w", indexName, err)
	}
	if res.IsError() && res.StatusCode != http.StatusNotFound {
		return xerrors.Errorf("error deleting index, index: %s, HTTP status: %s, err: %s", indexName, res.Status(), res.String())
	}
	return nil
}

func (s *Sink) updateIndexNameToIsStream(tableNameToIndexName map[abstract.TableID]string) error {
	for _, indexName := range tableNameToIndexName {
		_, found := s.indexNameToIsStream[indexName]
		if found {
			continue
		}
		result, err := isIndexStream(context.Background(), s.client, indexName)
		if err != nil {
			return xerrors.Errorf("unable to check if index %q is stream, err: %w", indexName, err)
		}
		s.indexNameToIsStream[indexName] = result
	}
	return nil
}

func (s *Sink) pushBatch(changeItems []abstract.ChangeItem) error {
	if len(changeItems) == 0 {
		return nil
	}
	indexResult := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tableNameToIndexName, err := buildTableIDToIndexName(changeItems)
	if err != nil {
		return xerrors.Errorf("unable to buildTableIdToIndexName, err: %w", err)
	}
	err = s.updateIndexNameToIsStream(tableNameToIndexName)
	if err != nil {
		return xerrors.Errorf("unable to updateIndexNameToIsStream, err: %w", err)
	}

	go func() {
		defer close(indexResult)

		indexer, _ := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
			Client:     s.client,
			NumWorkers: 1,
			OnError: func(ctx context.Context, err error) {
				indexResult <- xerrors.Errorf("indexer error: %w", err)
			},
		})

		for _, changeItem := range changeItems {
			if changeItem.Kind != abstract.InsertKind {
				continue
			}

			indexName, ok := tableNameToIndexName[changeItem.TableID()]
			if !ok {
				indexResult <- xerrors.Errorf("impossible situation - can't find indexName for tableName %s", changeItem.TableID().String())
				break
			}

			encodedBody, err := makeIndexBodyFromChangeItem(changeItem)
			if err != nil {
				indexResult <- xerrors.Errorf("can't make index request body from change item: %w", err)
				break
			}

			if s.cfg.SanitizeDocKeys {
				if clearedEncodedBody, err := sanitizeKeysInRawJSON(encodedBody); err == nil {
					encodedBody = clearedEncodedBody
				}
			}

			action := "index"
			isDataStream, found := s.indexNameToIsStream[indexName]
			if !found {
				indexResult <- xerrors.Errorf("impossible situation - can't find indexNameToIsStream for tableName %s", changeItem.TableID().String())
				break
			}
			if isDataStream {
				action = "create"
			}

			err = indexer.Add(
				ctx,
				esutil.BulkIndexerItem{
					Index:      indexName,
					Action:     action,
					DocumentID: makeIDFromChangeItem(changeItem),
					Body:       bytes.NewReader(encodedBody),
					OnFailure: func(_ context.Context, bulkItem esutil.BulkIndexerItem, responseItem esutil.BulkIndexerResponseItem, err error) {
						// centralized error classification for bulk item failures
						indexResult <- classifyBulkFailure(bulkItem, responseItem, err)
					},
				})
			if err != nil {
				indexResult <- xerrors.Errorf("can't add item to index: %w", err)
				break
			}
		}
		indexResult <- indexer.Close(ctx)
	}()

	for err := range indexResult {
		if err != nil {
			return xerrors.Errorf("can't index document: %w", err)
		}
	}

	s.logger.Info("Pushed", log.Any("count", len(changeItems)))
	return nil
}

func (s *Sink) Close() error {
	return nil
}

func NewSinkImpl(cfg *ElasticSearchDestination, logger log.Logger, registry core_metrics.Registry, client *elasticsearch.Client) (abstract.Sinker, error) {
	return &Sink{
		cfg:                 cfg,
		client:              client,
		logger:              logger,
		stats:               stats.NewSinkerStats(registry),
		existsIndexes:       set.New[abstract.TableID](),
		indexNameToIsStream: make(map[string]bool),
	}, nil
}

func NewSink(cfg *ElasticSearchDestination, logger log.Logger, registry core_metrics.Registry) (abstract.Sinker, error) {
	config, err := ConfigFromDestination(logger, cfg, ElasticSearch)
	if err != nil {
		return nil, xerrors.Errorf("failed to create elastic configuration: %w", err)
	}
	client, err := WithLogger(*config, log.With(logger, log.Any("component", "esclient")), ElasticSearch)
	if err != nil {
		return nil, xerrors.Errorf("failed to create elastic client: %w", err)
	}
	return NewSinkImpl(cfg, logger, registry, client)
}
