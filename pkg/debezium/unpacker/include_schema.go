package unpacker

import (
	"encoding/json"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/util"
)

type IncludeSchema struct {
}

func (*IncludeSchema) Unpack(message []byte) ([]byte, []byte, error) {
	var jsonMessage struct {
		Schema  json.RawMessage `json:"schema"`
		Payload json.RawMessage `json:"payload"`
	}
	err := json.Unmarshal(message, &jsonMessage)
	if err != nil {
		return nil, nil, xerrors.Errorf("can't decode message %s: %w", util.Sample(string(message), 1024), err)
	}

	return jsonMessage.Schema, jsonMessage.Payload, nil
}

func NewIncludeSchema() *IncludeSchema {
	return &IncludeSchema{}
}
