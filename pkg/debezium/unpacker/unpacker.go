package unpacker

import (
	"github.com/transferria/transferria/pkg/schemaregistry/confluent"
)

type Unpacker interface {
	// Unpack schema and payload from message
	Unpack(message []byte) (schema []byte, payload []byte, err error)
}

func NewMessageUnpacker(srClient *confluent.SchemaRegistryClient) Unpacker {
	if srClient != nil {
		return NewSchemaRegistry(srClient)
	}
	return NewIncludeSchema()
}
