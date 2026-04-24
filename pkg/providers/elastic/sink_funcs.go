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

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/errors/coded"
	error_codes "github.com/transferia/transferia/pkg/errors/codes"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/jsonx"
)

func makeIndexNameFromTableID(id abstract.TableID) (string, error) {
	var out string
	if id.Namespace == "" {
		out = id.Name
	} else if id.Name == "" {
		out = id.Namespace
	} else {
		out = id.Namespace + "." + id.Name
	}

	// limitations described here: https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create

	if out == "" || out == "." || out == ".." {
		return "", xerrors.Errorf("index name (%v) can't be empty, . or ..", out)
	}

	out = strings.ToLower(out)
	const illegalSymbols = `\/*?"<>| ,#:`
	if strings.ContainsAny(out, illegalSymbols) {
		return "", xerrors.Errorf("index name (%v) can't contains symbols: %v", out, illegalSymbols)
	}

	const illegalStartSymbols = `-_+`
	if strings.ContainsAny(string(out[0]), illegalStartSymbols) {
		return "", xerrors.Errorf("index name (%v) can't starts with: %v", out, illegalStartSymbols)
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

// classifyBulkFailure converts a bulk index failure into a coded error when possible.
// It inspects transport errors, known OpenSearch/Elastic messages and maps them to stable codes.
func classifyBulkFailure(bulkItem esutil.BulkIndexerItem, responseItem esutil.BulkIndexerResponseItem, err error) error {
	// read (sampled) body for context
	var bulkBody string
	buf := new(bytes.Buffer)
	if _, readErr := buf.ReadFrom(bulkItem.Body); readErr == nil {
		bulkBody = buf.String()
	}

	// Transport-layer error
	if err != nil {
		if isSSLError(err) {
			return coded.Errorf(error_codes.OpenSearchSSLRequired, "ssl/transport error (index:%v, body:%v): %v", bulkItem.Index, util.Sample(bulkBody, 8*1024), err)
		}
		return xerrors.Errorf("bulk item (index name:%v, body:%v) indexation error: %w", bulkItem.Index, util.Sample(bulkBody, 8*1024), err)
	}

	// Response-level error
	reason := responseItem.Error.Reason
	cause := responseItem.Error.Cause.Reason
	errText := strings.ToLower(reason + " " + cause)

	// invalid document keys (already existed path)
	if util.ContainsAnySubstrings(errText, "object field starting or ending with a [.] makes object resolution ambiguous", "index -1 out of bounds for length 0") {
		return coded.Errorf(error_codes.OpenSearchInvalidDocumentKeys,
			"invalid document keys for a bulk item (index:%v, body:%v) http:%v, err:%v",
			bulkItem.Index, util.Sample(bulkBody, 8*1024), responseItem.Status, responseItem.Error)
	}

	// total fields limit exceeded
	if responseItem.Error.Type == "illegal_argument_exception" || util.ContainsAnySubstrings(errText, "limit of total fields") {
		return coded.Errorf(error_codes.OpenSearchTotalFieldsLimitExceeded,
			"total fields limit exceeded (index:%v, body:%v) http:%v, err:%v",
			bulkItem.Index, util.Sample(bulkBody, 8*1024), responseItem.Status, responseItem.Error)
	}

	// mapper parsing exception
	if responseItem.Error.Type == "mapper_parsing_exception" || util.ContainsAnySubstrings(errText, "mapper_parsing_exception", "failed to parse field") {
		return coded.Errorf(error_codes.OpenSearchMapperParsingException,
			"mapper parsing failed (index:%v, body:%v) http:%v, err:%v",
			bulkItem.Index, util.Sample(bulkBody, 8*1024), responseItem.Status, responseItem.Error)
	}

	// ssl required hints surfaced at response level (rare)
	if containsSSLRequired(reason) || containsSSLRequired(cause) {
		return coded.Errorf(error_codes.OpenSearchSSLRequired,
			"ssl required (index:%v, body:%v) http:%v, err:%v",
			bulkItem.Index, util.Sample(bulkBody, 8*1024), responseItem.Status, responseItem.Error)
	}

	return xerrors.Errorf("got an indexation error for a bulk item (index name:%v, body:%v) with http code %v, error: %v",
		bulkItem.Index, util.Sample(bulkBody, 8*1024), responseItem.Status, responseItem.Error)
}

// isSSLError detects common TLS/SSL misconfiguration errors from client/transport
func isSSLError(err error) bool {
	if err == nil {
		return false
	}
	et := strings.ToLower(err.Error())
	return util.ContainsAnySubstrings(et, "x509:", "certificate", "ssl", "tls", "http: server gave http response to https client", "plain http request was sent to https port")
}

// containsSSLRequired checks response text for SSL-required markers
func containsSSLRequired(s string) bool {
	t := strings.ToLower(s)
	return strings.Contains(t, "ssl is required") || strings.Contains(t, "plain http request was sent to https port")
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

func buildTableIDToIndexName(changeItems []abstract.ChangeItem) (map[abstract.TableID]string, error) {
	result := make(map[abstract.TableID]string)
	for _, changeItem := range changeItems {
		currTableId := changeItem.TableID()
		if _, ok := result[currTableId]; ok {
			continue
		}
		indexName, err := makeIndexNameFromTableID(currTableId)
		if err != nil {
			return nil, xerrors.Errorf("failed to make index name for table id (%v), err: %w", changeItem.TableID(), err)
		}
		result[currTableId] = indexName
	}
	return result, nil
}

// isIndexStream - determines if index is OpenSearch Data streams
func isIndexStream(ctx context.Context, client *elasticsearch.Client, indexName string) (bool, error) {
	req := esapi.IndicesGetRequest{
		Index: []string{indexName},
	}

	res, err := req.Do(ctx, client)
	if err != nil {
		return false, xerrors.Errorf("failed to get index %s, err: %w", indexName, err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return false, nil
	}

	var indexInfo map[string]any
	if err = json.NewDecoder(res.Body).Decode(&indexInfo); err != nil {
		return false, xerrors.Errorf("failed to decode response body for index %s, err: %w", indexName, err)
	}

	// For data streams, GET /<stream> lists backing indices (e.g. ".ds-name-000001") as keys,
	// not the stream name. Those entries carry "data_stream" with the stream name (string).
	// Older responses might use the stream/index name as key and "is_data_stream" (bool).
	for key, idxRaw := range indexInfo {
		indexData, ok := idxRaw.(map[string]any)
		if !ok {
			continue
		}
		if ds, ok := indexData["data_stream"].(string); ok && ds == indexName {
			return true, nil
		}
		if key != indexName {
			continue
		}
		v, ok := indexData["is_data_stream"]
		if !ok {
			continue
		}
		boolFlag, ok := v.(bool)
		if !ok {
			continue
		}
		return boolFlag, nil
	}
	return false, nil
}
