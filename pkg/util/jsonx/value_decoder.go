package jsonx

import (
	"encoding/json"

	"github.com/transferria/transferria/library/go/core/xerrors"
)

// ValueDecoder is a decoder which deserializes JSONs into the Data Transfer's JSON representation
type ValueDecoder struct {
	base *json.Decoder
}

// NewValueDecoder constructs a decoder which deserializes JSONs into the Data Transfer's JSON representation
func NewValueDecoder(base *json.Decoder) *ValueDecoder {
	return &ValueDecoder{
		base: base,
	}
}

// Decode uses the decoder from the constructor to deserialize the input into a Data Transfer JSON
func (d *ValueDecoder) Decode() (any, error) {
	var result any
	if err := d.base.Decode(&result); err != nil {
		return nil, xerrors.Errorf("failed to decode JSON with base decoder: %w", err)
	}
	if result == nil {
		return JSONNull{}, nil
	}
	return result, nil
}
