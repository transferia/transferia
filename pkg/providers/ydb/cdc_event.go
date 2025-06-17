//go:build !disable_ydb_provider

package ydb

import "encoding/json"

type cdcEvent struct {
	Key      []interface{}          `json:"key"`
	Update   map[string]interface{} `json:"update"`
	Erase    map[string]interface{} `json:"erase"`
	NewImage map[string]interface{} `json:"newImage"`
	OldImage map[string]interface{} `json:"oldImage"`
}

func (c *cdcEvent) ToJSONString() string {
	result, _ := json.Marshal(c)
	return string(result)
}
