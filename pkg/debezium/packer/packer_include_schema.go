package packer

import (
	"encoding/json"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/util"
)

type PackerIncludeSchema struct{}

func (*PackerIncludeSchema) GetSchemaIDResolver() SchemaIDResolver { return nil }

func (s *PackerIncludeSchema) Pack(
	changeItem *abstract.ChangeItem,
	payloadBuilder BuilderFunc,
	kafkaSchemaBuilder BuilderFunc,
	maybeCachedRawSchema []byte,
) ([]byte, error) {
	rawPayload, err := payloadBuilder(changeItem)
	if err != nil {
		return nil, xerrors.Errorf("unable to build rawPayload, err: %w", err)
	}
	if maybeCachedRawSchema == nil {
		maybeCachedRawSchema, err = s.BuildFinalSchema(changeItem, kafkaSchemaBuilder)
		if err != nil {
			return nil, xerrors.Errorf("unable to build final schema, err: %w", err)
		}
	}
	jsonMessage := map[string]json.RawMessage{
		"schema":  json.RawMessage(maybeCachedRawSchema),
		"payload": json.RawMessage(rawPayload),
	}
	message, err := util.JSONMarshalUnescape(jsonMessage)
	if err != nil {
		return nil, xerrors.Errorf("unable to marshal result: %w", err)
	}
	return message, nil
}

func (s *PackerIncludeSchema) BuildFinalSchema(changeItem *abstract.ChangeItem, schemaBuilder BuilderFunc) ([]byte, error) {
	maybeCachedRawSchema, err := schemaBuilder(changeItem)
	if err != nil {
		return nil, xerrors.Errorf("unable to build schemaObj, err: %w", err)
	}
	return maybeCachedRawSchema, nil
}

func (s *PackerIncludeSchema) IsDropSchema() bool {
	return false
}

func NewPackerIncludeSchema() *PackerIncludeSchema {
	return &PackerIncludeSchema{}
}
