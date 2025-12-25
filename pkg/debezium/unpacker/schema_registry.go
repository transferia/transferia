package unpacker

import (
	"encoding/binary"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/schemaregistry/confluent"
	"github.com/transferia/transferia/pkg/util"
)

type SchemaRegistry struct {
	schemaRegistryClient *confluent.SchemaRegistryClient
}

func (s *SchemaRegistry) Unpack(message []byte) ([]byte, []byte, error) {
	if len(message) < 5 {
		return nil, nil, xerrors.Errorf("Can't extract schema id form message: message length less then 5 (%v)", len(message))
	}
	if message[0] != 0 {
		return nil, nil, xerrors.Errorf("Unknown magic byte in message (%v) (first byte in message must be 0)", string(message))
	}
	schemaID := binary.BigEndian.Uint32(message[1:5])

	schema, _ := backoff.RetryNotifyWithData(func() (*confluent.Schema, error) {
		return s.schemaRegistryClient.GetSchema(int(schemaID))
	}, backoff.NewConstantBackOff(time.Second), util.BackoffLogger(logger.Log, "getting schema"))

	return []byte(schema.Schema), message[5:], nil
}

func (s *SchemaRegistry) SchemaRegistryClient() *confluent.SchemaRegistryClient {
	return s.schemaRegistryClient
}

func NewSchemaRegistry(srClient *confluent.SchemaRegistryClient) *SchemaRegistry {
	return &SchemaRegistry{
		schemaRegistryClient: srClient,
	}
}
