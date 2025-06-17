//go:build !disable_elastic_provider

package elastic

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/jsonx"
	"github.com/transferia/transferia/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
)

type Sink struct {
	cfg    *ElasticSearchDestination
	client *elasticsearch.Client
	logger log.Logger
	stats  *stats.SinkerStats

	existsIndexes      *set.Set[abstract.TableID]
	existsIndexesMutex sync.RWMutex
}

func makeIndexNameFromTableID(id abstract.TableID) (string, error) {
	var out string
	if id.Namespace == "" {
		out = id.Name
	} else if id.Name == "" {
		out = id.Namespace
	} else {
		out = id.Namespace + "." + id.Name
	}

	if out == "" || out == "." || out == ".." {
		return "", xerrors.Errorf("index name (%v) can't be empty, . or ..", out)
	}

	out = strings.ToLower(out)
	const illegalSymbols = `\/*?"<>| ,#:`
	if strings.ContainsAny(out, illegalSymbols) {
		return "", xerrors.Errorf("index name (%v) can't contains symbols: %v", out, illegalSymbols)
	}

	const illegalStartSymbols = `-_+`
	for i := range []byte(illegalStartSymbols) {
		if out[0] == illegalStartSymbols[i] {
			return "", xerrors.Errorf("index name (%v) can't starts with: %v", out, illegalStartSymbols)
		}
	}
	return out, nil
}

func makeIDFromChangeItem(changeItem abstract.ChangeItem) string {
	primaryKeys := changeItem.KeyVals()
	if len(primaryKeys) == 0 {
		return ""
	}
	const concatSymbol = "."
	if len(primaryKeys) > 0 {
		for i := range primaryKeys {
			primaryKeys[i] = strings.ReplaceAll(primaryKeys[i], concatSymbol, "\\"+concatSymbol)
		}
	}
	idField := url.QueryEscape(strings.Join(primaryKeys, concatSymbol))
	if len(idField) > 512 {
		h := sha1.New()
		h.Write([]byte(idField))
		idField = url.QueryEscape(hex.EncodeToString(h.Sum(nil)))
	}
	return idField
}

func (s *Sink) applyIndexDump(item abstract.ChangeItem) error {
	if item.Kind != abstract.ElasticsearchDumpIndexKind {
		return nil
	}
	tableID := item.TableID()
	s.existsIndexesMutex.RLock()
	if s.existsIndexes.Contains(tableID) {
		s.existsIndexesMutex.RUnlock()
		return nil
	}
	s.existsIndexesMutex.RUnlock()

	indexName, _ := makeIndexNameFromTableID(tableID)

	response, err := s.client.Indices.Exists([]string{indexName})
	if err != nil {
		return xerrors.Errorf("unable to check if index %q exists: %w", indexName, err)
	}
	if response.StatusCode == http.StatusOK {
		s.existsIndexesMutex.Lock()
		defer s.existsIndexesMutex.Unlock()
		s.existsIndexes.Add(tableID)
		return nil
	}
	if response.StatusCode != http.StatusNotFound {
		return xerrors.Errorf("wrong status code when checking index %q: %s", indexName, response.String())
	}

	//
	dumpParams, ok := item.ColumnValues[0].(string)

	if !ok {
		return xerrors.Errorf("unable to extract the index dump data: %v, %T", item.ColumnValues[0], item.ColumnValues[0])
	}

	res, err := s.client.Indices.Create(indexName,
		s.client.Indices.Create.WithMasterTimeout(time.Second*30),
		s.client.Indices.Create.WithBody(strings.NewReader(dumpParams)),
	)
	if err != nil {
		return xerrors.Errorf("unable to create the index %q: %w", indexName, err)
	}
	if res.IsError() {
		return xerrors.Errorf("error on creating the index %q: %s", indexName, res.String())
	}

	// wait until the index creation is applied
	err = WaitForIndexToExist(s.client, indexName, time.Second*30)
	if err != nil {
		return xerrors.Errorf("elastic check index creating error: %w", err)
	}

	s.existsIndexesMutex.Lock()
	defer s.existsIndexesMutex.Unlock()
	s.existsIndexes.Add(tableID)
	return nil
}

func makeIndexBodyFromChangeItem(changeItem abstract.ChangeItem) ([]byte, error) {
	itemMap := changeItem.AsMap()
	systemInfo := map[string]interface{}{
		"schema": changeItem.Schema,
		"table":  changeItem.Table,
		"id":     changeItem.ID,
	}
	if idField, ok := itemMap["_id"]; ok {
		systemInfo["original_id"] = idField
		delete(itemMap, "_id")
	}
	itemMap["__data_transfer"] = systemInfo
	bytesToStringInMapValues(itemMap)
	js, err := json.Marshal(itemMap)
	if err != nil {
		return nil, xerrors.Errorf("unable to encode message: %w", err)
	}
	return js, nil
}

// json.Marshal converts []byte to base64 form.
// bytesToStringInMapValues should fix it
func bytesToStringInMapValues(itemMap map[string]interface{}) {
	if itemMap == nil {
		return
	}
	for key, val := range itemMap {
		switch typedVal := val.(type) {
		case map[string]interface{}:
			bytesToStringInMapValues(itemMap[key].(map[string]interface{}))
		case []byte:
			itemMap[key] = string(typedVal)
		}
	}
}

func sanitizeKeysInRawJSON(rawJSON []byte) ([]byte, error) {
	var decodedJSON map[string]interface{}
	if err := jsonx.Unmarshal(rawJSON, &decodedJSON); err != nil {
		return nil, xerrors.Errorf("can't unmarshal a json string: %w", err)
	}

	toClear := []map[string]interface{}{decodedJSON}
	for len(toClear) > 0 {
		toClear = append(toClear[:len(toClear)-1], sanitizeKeysInMap(toClear[len(toClear)-1])...)
	}

	out, err := json.Marshal(decodedJSON)
	if err != nil {
		return nil, xerrors.Errorf("can't marshal a struct into json: %w", err)
	}
	return out, nil
}

func sanitizeKeysInMap(in map[string]interface{}) []map[string]interface{} {
	var mapsInside []map[string]interface{}
	mapKeys := make([]string, 0, len(in))
	for key := range in {
		mapKeys = append(mapKeys, key)
	}
	for _, key := range mapKeys {
		if mapInside, ok := in[key].(map[string]interface{}); ok {
			mapsInside = append(mapsInside, mapInside)
		}
		if newKey := sanitizeMapKey(key); newKey != key {
			in[newKey] = in[key]
			delete(in, key)
		}
	}
	return mapsInside
}

func sanitizeMapKey(in string) string {
	runes := []rune(in)
	outStringLen := 0

	startCopyStr := 0
	isEmptyCopyStr := true
	for i := 0; i <= len(runes); i++ {
		if i == len(runes) || runes[i] == '.' {
			if !isEmptyCopyStr {
				if outStringLen != 0 {
					runes[outStringLen] = '.'
					outStringLen++
				}
				for j := startCopyStr; j < i; j++ {
					runes[outStringLen] = runes[j]
					outStringLen++
				}
			}
			startCopyStr = i + 1
			isEmptyCopyStr = true
			continue
		}
		if runes[i] != ' ' {
			isEmptyCopyStr = false
		}
	}
	if outStringLen != 0 {
		return string(runes[:outStringLen])
	}
	return "_"
}

func validateChangeItem(changeItem abstract.ChangeItem) error {
	switch changeItem.Kind {
	case abstract.DeleteKind, abstract.UpdateKind:
		return xerrors.Errorf("update/delete kinds for now is not supported")
	case abstract.TruncateTableKind:
		return xerrors.Errorf("truncate is not supported for elastic/opensearch for now")
	default:
		return nil
	}
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

func (s *Sink) pushBatch(changeItems []abstract.ChangeItem) error {
	if len(changeItems) == 0 {
		return nil
	}
	indexResult := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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

			indexName, err := makeIndexNameFromTableID(changeItem.TableID())
			if err != nil {
				indexResult <- xerrors.Errorf("can't make index name from %v: %w", changeItem.TableID().String(), err)
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

			err = indexer.Add(
				ctx,
				esutil.BulkIndexerItem{
					Index:      indexName,
					Action:     "index",
					DocumentID: makeIDFromChangeItem(changeItem),
					Body:       bytes.NewReader(encodedBody),
					OnFailure: func(_ context.Context, bulkItem esutil.BulkIndexerItem, responseItem esutil.BulkIndexerResponseItem, err error) {
						var bulkBody string
						buf := new(bytes.Buffer)
						if _, readErr := buf.ReadFrom(bulkItem.Body); readErr == nil {
							bulkBody = buf.String()
						}
						if err != nil {
							indexResult <- xerrors.Errorf("bulk item (index name:%v, body:%v) indexation error: %w",
								bulkItem.Index, util.Sample(bulkBody, 8*1024), err)
							return
						}
						indexResult <- xerrors.Errorf(
							"got an indexation error for a bulk item (index name:%v, body:%v) with http code %v, error: %v",
							bulkItem.Index, util.Sample(bulkBody, 8*1024), responseItem.Status, responseItem.Error)
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

func NewSinkImpl(cfg *ElasticSearchDestination, logger log.Logger, registry metrics.Registry, client *elasticsearch.Client) (abstract.Sinker, error) {
	return &Sink{
		cfg:                cfg,
		client:             client,
		logger:             logger,
		stats:              stats.NewSinkerStats(registry),
		existsIndexes:      set.New[abstract.TableID](),
		existsIndexesMutex: sync.RWMutex{},
	}, nil
}

func NewSink(cfg *ElasticSearchDestination, logger log.Logger, registry metrics.Registry) (abstract.Sinker, error) {
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
