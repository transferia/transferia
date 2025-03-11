package packer

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
	debeziumparameters "github.com/transferria/transferria/pkg/debezium/parameters"
)

type PackerSchemaRegistryMocked struct {
	t          *testing.T
	packerImpl Packer
}

func (s *PackerSchemaRegistryMocked) Pack(
	changeItem *abstract.ChangeItem,
	payloadBuilder BuilderFunc,
	kafkaSchemaBuilder BuilderFunc,
	_ []byte,
) ([]byte, error) {
	finalSchema, err := s.BuildFinalSchema(changeItem, kafkaSchemaBuilder)
	require.NoError(s.t, err)
	schemaID, err := s.ResolveSchemaID(finalSchema, changeItem.TableID())
	require.NoError(s.t, err)
	payload, err := payloadBuilder(changeItem)
	require.NoError(s.t, err)
	return s.packerImpl.GetSchemaIDResolver().PackWithSchemaID(schemaID, payload)
}

func (s *PackerSchemaRegistryMocked) BuildFinalSchema(changeItem *abstract.ChangeItem, kafkaSchemaBuilder BuilderFunc) ([]byte, error) {
	return s.packerImpl.BuildFinalSchema(changeItem, kafkaSchemaBuilder)
}

func (s *PackerSchemaRegistryMocked) PackWithSchemaID(schemaID uint32, payload []byte) ([]byte, error) {
	return s.packerImpl.GetSchemaIDResolver().PackWithSchemaID(schemaID, payload)
}

func (s *PackerSchemaRegistryMocked) ResolveSchemaID(_ []byte, _ abstract.TableID) (uint32, error) {
	return 9, nil
}

func (s *PackerSchemaRegistryMocked) GetSchemaIDResolver() SchemaIDResolver { return s }

func NewPackerSchemaRegistryMocked(t *testing.T, packer Packer) *PackerSchemaRegistryMocked {
	return &PackerSchemaRegistryMocked{
		t:          t,
		packerImpl: packer,
	}
}

//---

func TestPackerSchemaRegistry(t *testing.T) {
	packerSchemaRegistry := NewPackerSchemaRegistry(nil, debeziumparameters.SubjectTopicRecordNameStrategy, true, true, "my_topic_name", false)
	packerSchemaRegistryMocked := NewPackerSchemaRegistryMocked(t, packerSchemaRegistry)
	result, err := packerSchemaRegistryMocked.Pack(
		getTestChangeItem(),
		func(changeItem *abstract.ChangeItem) ([]byte, error) { return []byte{1}, nil },                       // payload
		func(changeItem *abstract.ChangeItem) ([]byte, error) { return []byte(`{"field": "my_field"}`), nil }, // schema
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, []byte{0x0, 0x0, 0x0, 0x0, 0x9, 0x1}, result)
}
