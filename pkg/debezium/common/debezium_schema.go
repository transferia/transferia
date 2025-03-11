package common

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/util/jsonx"
)

type Schema struct {
	Field              string            `json:"field"`
	Fields             []Schema          `json:"fields"`
	Name               string            `json:"name"`
	Optional           bool              `json:"optional"`
	Parameters         *SchemaParameters `json:"parameters,omitempty"`
	Type               string            `json:"type"`
	Version            int               `json:"version"`
	Items              *Schema           `json:"items"`
	DTOriginalTypeInfo *OriginalTypeInfo `json:"__dt_original_type_info"`
}

type SchemaParameters struct {
	Length                  string `json:"length,omitempty"`
	ConnectDecimalPrecision string `json:"connect.decimal.precision,omitempty"`
	Scale                   string `json:"scale,omitempty"`
	Allowed                 string `json:"allowed,omitempty"`
}

type Source struct {
	Connector string `json:"connector"`
	DB        string `json:"db"`
	LSN       uint64 `json:"lsn"`
	Name      string `json:"name"`
	Schema    string `json:"schema"`
	Sequence  string `json:"sequence"`
	Snapshot  string `json:"snapshot"`
	Table     string `json:"table"`
	TSMs      uint64 `json:"ts_ms"`
	TXID      uint32 `json:"txId"`
	Version   string `json:"version"`
	XMin      *int   `json:"xmin"`
}

type Payload struct {
	After       map[string]interface{} `json:"after"`
	Before      map[string]interface{} `json:"before"`
	Op          string                 `json:"op"`
	Source      Source                 `json:"source"`
	Transaction json.RawMessage        `json:"transaction"`
	TSMs        uint64                 `json:"ts_ms"`
}

type Message struct {
	Payload Payload `json:"payload"`
	Schema  Schema  `json:"schema"`
}

//---

func UnmarshalPayload(in []byte) (*Payload, error) {
	var payload Payload
	if err := jsonx.NewDefaultDecoder(bytes.NewReader(in)).Decode(&payload); err != nil {
		return nil, xerrors.Errorf("unable to unmarshal payload - err: %w", err)
	}
	return &payload, nil
}

func UnmarshalSchema(in []byte) (*Schema, error) {
	var schema Schema
	if err := json.Unmarshal(in, &schema); err != nil {
		return nil, xerrors.Errorf("unable to unmarshal schema - err: %w", err)
	}
	return &schema, nil
}

func UnmarshalMessage(in string) (*Message, error) {
	var msg Message
	if err := jsonx.NewDefaultDecoder(strings.NewReader(in)).Decode(&msg); err != nil {
		return nil, xerrors.Errorf("unable to unmarshal changeItems - err: %w", err)
	}
	return &msg, nil
}

func (schema *Schema) FindBeforeSchema() *Schema {
	return schema.FindSchemaDescr("before")
}

func (schema *Schema) FindAfterSchema() *Schema {
	return schema.FindSchemaDescr("after")
}

func (schema *Schema) FindSchemaDescr(fieldName string) *Schema {
	for i := range schema.Fields {
		if schema.Fields[i].Field == fieldName {
			return &schema.Fields[i]
		}
	}
	return nil
}
