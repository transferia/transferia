package packer

import (
	"bytes"
	"encoding/binary"
	"encoding/json"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/schemaregistry/confluent"
	"github.com/transferria/transferria/pkg/schemaregistry/format"
)

type PackerSchemaRegistry struct {
	schemaRegistryClient   *confluent.SchemaRegistryClient
	isKeyProcessor         bool
	subjectNameStrategy    string
	writeIntoOneTopic      bool
	topic                  string // full topic name or prefix
	makeClosedContentModel bool
}

func (s *PackerSchemaRegistry) Pack(
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
	schemaID, err := s.ResolveSchemaID(maybeCachedRawSchema, changeItem.TableID())
	if err != nil {
		return nil, xerrors.Errorf("unable to get schemaID, err: %w", err)
	}
	return s.PackWithSchemaID(schemaID, rawPayload)
}

func (s *PackerSchemaRegistry) BuildFinalSchema(changeItem *abstract.ChangeItem, kafkaSchemaBuilder BuilderFunc) ([]byte, error) {
	schemaObj, err := kafkaSchemaBuilder(changeItem)
	if err != nil {
		return nil, xerrors.Errorf("can't build schemaObj object: %w", err)
	}
	kafkaSchema, err := format.KafkaJSONSchemaFromArr(schemaObj)
	if err != nil {
		return nil, xerrors.Errorf("can't convert map into kafka json schema: %w", err)
	}
	rawSchema, err := json.Marshal(kafkaSchema.ToConfluentSchema(s.makeClosedContentModel))
	if err != nil {
		return nil, xerrors.Errorf("unable to marshal schema in confluent json format: %w", err)
	}
	return rawSchema, nil
}

func (s *PackerSchemaRegistry) IsDropSchema() bool {
	return false
}

func (s *PackerSchemaRegistry) PackWithSchemaID(schemaID uint32, payload []byte) ([]byte, error) {
	var resultBuf bytes.Buffer
	resultBuf.WriteByte(0)
	if err := binary.Write(&resultBuf, binary.BigEndian, schemaID); err != nil {
		return nil, xerrors.Errorf("can't encode a schema ID in a payload: %w", err)
	}
	if _, err := resultBuf.Write(payload); err != nil {
		return nil, xerrors.Errorf("can't write payload: %w", err)
	}
	return resultBuf.Bytes(), nil
}

func (s *PackerSchemaRegistry) ResolveSchemaID(schema []byte, table abstract.TableID) (uint32, error) {
	subjectName := makeSubjectName(table, s.topic, s.writeIntoOneTopic, s.isKeyProcessor, s.subjectNameStrategy)
	schemaID, err := s.schemaRegistryClient.CreateSchema(subjectName, string(schema), confluent.JSON)
	if err != nil {
		return 0, xerrors.Errorf("can't push schema into the schema registry: %w", err)
	}
	return uint32(schemaID), nil
}

func (s *PackerSchemaRegistry) GetSchemaIDResolver() SchemaIDResolver { return s }

func NewPackerSchemaRegistry(
	srClient *confluent.SchemaRegistryClient,
	subjectNameStrategy string,
	isKeyProcessor bool,
	writeIntoOneTopic bool,
	topic string,
	makeClosedContentModel bool,
) *PackerSchemaRegistry {
	return &PackerSchemaRegistry{
		schemaRegistryClient:   srClient,
		isKeyProcessor:         isKeyProcessor,
		subjectNameStrategy:    subjectNameStrategy,
		writeIntoOneTopic:      writeIntoOneTopic,
		topic:                  topic,
		makeClosedContentModel: makeClosedContentModel,
	}
}
