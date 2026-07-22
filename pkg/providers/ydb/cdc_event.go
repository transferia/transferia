package ydb

import "encoding/json"

type cdcEvent struct {
	Key       []interface{}          `json:"key"`
	Update    map[string]interface{} `json:"update"`
	Erase     map[string]interface{} `json:"erase"`
	NewImage  map[string]interface{} `json:"newImage"`
	OldImage  map[string]interface{} `json:"oldImage"`
	Timestamp []uint64               `json:"ts"` // VIRUTAL_TIMESTAMP has format [<step>, <txId>] https://ydb.yandex-team.ru/docs/concepts/cdc#json-record-structure
}

func (c *cdcEvent) ToJSONString() string {
	result, _ := json.Marshal(c)
	return string(result)
}
