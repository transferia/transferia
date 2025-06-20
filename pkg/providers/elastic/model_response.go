//go:build !disable_elastic_provider

package elastic

import "encoding/json"

type total struct {
	Value int `json:"value"`
}

type hit struct {
	Index  string          `json:"_index"`
	ID     string          `json:"_id"`
	Type   string          `json:"_type"`
	Source json.RawMessage `json:"_source"`
}
type searchResults struct {
	Hits  []hit `json:"hits"`
	Total total `json:"total"`
}

type mappingType struct {
	Properties map[string]json.RawMessage `json:"properties"`
	Type       string                     `json:"type"`
	Format     string                     `json:"format"`
	Path       string                     `json:"path"`
}

type mappingProperties struct {
	Properties map[string]mappingType `json:"properties"`
}
type mapping struct {
	Mappings mappingProperties `json:"mappings"`
}

type healthResponse struct {
	Shards int `json:"active_shards"`
}

type searchResponse struct {
	ScrollID string        `json:"_scroll_id"`
	Hits     searchResults `json:"hits"`
}

type countResponse struct {
	Count uint64 `json:"count"`
}

type statsResponse struct {
	Indices map[string]interface{} `json:"indices"`
}
