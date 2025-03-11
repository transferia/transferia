package debezium

import (
	"encoding/json"
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	debeziumcommon "github.com/transferia/transferia/pkg/debezium/common"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
	"github.com/transferia/transferia/pkg/debezium/unpacker"
	"github.com/transferia/transferia/pkg/schemaregistry/confluent"
	"github.com/transferia/transferia/pkg/schemaregistry/format"
	"github.com/transferia/transferia/pkg/util"
)

type Receiver struct {
	originalTypes         map[abstract.TableID]map[string]*debeziumcommon.OriginalTypeInfo // map: table -> [fieldName -> originalType]
	Unpacker              unpacker.Unpacker
	schemaFormat          string
	tableSchemaCache      map[string]tableSchemaCacheItem
	tableSchemaCacheMutex *sync.RWMutex
}

type tableSchemaCacheItem struct {
	beforeSchema   *abstract.TableSchema
	afterSchema    *abstract.TableSchema
	debeziumSchema *debeziumcommon.Schema
}

func (r *Receiver) originalType(schema, tableName, fieldName string) *debeziumcommon.OriginalTypeInfo {
	currTableID := abstract.TableID{
		Namespace: schema,
		Name:      tableName,
	}
	if fieldToOriginalType, ok := r.originalTypes[currTableID]; ok {
		if currOriginalTypeInfo, ok := fieldToOriginalType[fieldName]; ok {
			return currOriginalTypeInfo
		}
	}
	return &debeziumcommon.OriginalTypeInfo{OriginalType: "", Properties: nil}
}

func (r *Receiver) receiveTableSchema(schema *debeziumcommon.Schema, schemaName, tableName string) (*abstract.TableSchema, error) {
	var tableSchema []abstract.ColSchema
	for i := range schema.Fields {
		originalType := r.originalType(schemaName, tableName, schema.Fields[i].Field)
		if schema.Fields[i].DTOriginalTypeInfo != nil {
			originalType = schema.Fields[i].DTOriginalTypeInfo
		}
		newColSchema, err := receiveFieldColSchema(&schema.Fields[i], originalType)
		if err != nil {
			return nil, xerrors.Errorf("unable to receive col schema, err: %w", err)
		}

		newColSchema.TableSchema = schemaName
		newColSchema.TableName = tableName
		tableSchema = append(tableSchema, *newColSchema)
	}
	return abstract.NewTableSchema(tableSchema), nil
}
func (r *Receiver) receiveSchema(schema []byte, tableName, schemaName string) (*abstract.TableSchema, *abstract.TableSchema, *debeziumcommon.Schema, error) {
	r.tableSchemaCacheMutex.RLock()
	cache, ok := r.tableSchemaCache[util.Hash(string(schema))]
	r.tableSchemaCacheMutex.RUnlock()
	if ok {
		return cache.beforeSchema, cache.afterSchema, cache.debeziumSchema, nil
	}
	convertedSchema, err := r.convertSchemaFormat(schema)
	if err != nil {
		return nil, nil, nil, xerrors.Errorf("can't convert schema format: %w", err)
	}
	debeziumSchema, err := debeziumcommon.UnmarshalSchema(convertedSchema)
	if err != nil {
		return nil, nil, nil, xerrors.Errorf("can't unmarshal converted schema: %w", err)
	}
	beforeSchema, err := r.receiveTableSchema(debeziumSchema.FindBeforeSchema(), schemaName, tableName)
	if err != nil {
		return nil, nil, nil, xerrors.New("unable to receive 'before' schema")
	}
	afterSchema, err := r.receiveTableSchema(debeziumSchema.FindAfterSchema(), schemaName, tableName)
	if err != nil {
		return nil, nil, nil, xerrors.New("unable to receive 'after' schema")
	}

	r.tableSchemaCacheMutex.Lock()
	r.tableSchemaCache[util.Hash(string(schema))] = tableSchemaCacheItem{
		beforeSchema:   beforeSchema,
		afterSchema:    afterSchema,
		debeziumSchema: debeziumSchema,
	}
	r.tableSchemaCacheMutex.Unlock()

	return beforeSchema, afterSchema, debeziumSchema, nil
}

func (r *Receiver) add(changeItem *abstract.ChangeItem, inSchemaDescr *debeziumcommon.Schema, inVal interface{}, originalType *debeziumcommon.OriginalTypeInfo, kind abstract.Kind, ytType string, isKey bool) error {
	resultVal, isAbsent, err := receiveFieldChecked(ytType, inSchemaDescr, inVal, originalType)
	if err != nil {
		return xerrors.Errorf("unable to get field description, err: %w", err)
	}
	if isAbsent {
		return nil
	}

	if kind == abstract.InsertKind || kind == abstract.UpdateKind {
		changeItem.ColumnNames = append(changeItem.ColumnNames, inSchemaDescr.Field)
		changeItem.ColumnValues = append(changeItem.ColumnValues, resultVal)
	}

	if kind == abstract.UpdateKind || kind == abstract.DeleteKind {
		if isKey {
			changeItem.OldKeys.KeyNames = append(changeItem.OldKeys.KeyNames, inSchemaDescr.Field)
			changeItem.OldKeys.KeyValues = append(changeItem.OldKeys.KeyValues, resultVal)
			changeItem.OldKeys.KeyTypes = append(changeItem.OldKeys.KeyTypes, ytType)
		}
	}

	return nil
}

func (r *Receiver) convertSchemaFormat(schema []byte) ([]byte, error) {
	switch r.schemaFormat {
	case debeziumparameters.ConverterConfluentJSON:
		var confluentSchema format.ConfluentJSONSchema
		if err := json.Unmarshal(schema, &confluentSchema); err != nil {
			return nil, xerrors.Errorf("can't unmarshal confluent schema: %w", err)
		}
		rawSchema, err := json.Marshal(confluentSchema.ToKafkaJSONSchema())
		if err != nil {
			return nil, xerrors.Errorf("unable to marshal schema in confluent json format: %w", err)
		}
		return rawSchema, nil
	case debeziumparameters.ConverterApacheKafkaJSON:
		fallthrough
	default:
		return schema, nil
	}
}

func (r *Receiver) Receive(in string) (*abstract.ChangeItem, error) {
	schema, payload, err := r.Unpacker.Unpack([]byte(in))
	if err != nil {
		return nil, xerrors.Errorf("can't unpack message: %w", err)
	}

	return r.receive(schema, payload)
}

func (r *Receiver) receive(schema, payload []byte) (*abstract.ChangeItem, error) {
	payloadStruct, err := debeziumcommon.UnmarshalPayload(payload)
	if err != nil {
		return nil, xerrors.Errorf("unable to unmarshal json payload: %s, err: %w", string(payload), err)
	}
	kind, err := opToKind(payloadStruct.Op)
	if err != nil {
		return nil, xerrors.Errorf("unable to determine kind, err: %w", err)
	}
	switch kind {
	case abstract.InsertKind, abstract.UpdateKind, abstract.DeleteKind:
	default:
		return nil, xerrors.Errorf("unsupported kind: %s", string(kind))
	}
	beforeSchema, afterSchema, debeziumSchema, err := r.receiveSchema(schema, payloadStruct.Source.Table, payloadStruct.Source.Schema)
	if err != nil {
		return nil, xerrors.Errorf("schema receiving errpo: %w", err)
	}
	var currValuesMap map[string]interface{}
	var currTableSchema *abstract.TableSchema
	var currDebeziumSchema *debeziumcommon.Schema
	if kind == abstract.DeleteKind {
		currValuesMap = payloadStruct.Before
		currTableSchema = beforeSchema
		currDebeziumSchema = debeziumSchema.FindBeforeSchema()
	} else {
		currValuesMap = payloadStruct.After
		currTableSchema = afterSchema
		currDebeziumSchema = debeziumSchema.FindAfterSchema()
	}

	result := &abstract.ChangeItem{
		ID:           payloadStruct.Source.TXID,
		LSN:          payloadStruct.Source.LSN,
		CommitTime:   payloadStruct.Source.TSMs * 1000000,
		Counter:      0, // TODO
		Kind:         kind,
		Schema:       payloadStruct.Source.Schema,
		Table:        payloadStruct.Source.Table,
		PartID:       "", // TODO
		ColumnNames:  nil,
		ColumnValues: nil,
		TableSchema:  currTableSchema,
		OldKeys: abstract.OldKeysType{
			KeyNames:  nil,
			KeyTypes:  nil,
			KeyValues: nil,
		},
		TxID:  "",
		Query: "",
		Size:  abstract.RawEventSize(util.DeepSizeof(payload)),
	}
	for i := range currDebeziumSchema.Fields {
		if val, ok := currValuesMap[currDebeziumSchema.Fields[i].Field]; ok {
			originalType := r.originalType(payloadStruct.Source.Schema, payloadStruct.Source.Table, currDebeziumSchema.Fields[i].Field)
			if currDebeziumSchema.Fields[i].DTOriginalTypeInfo != nil {
				originalType = currDebeziumSchema.Fields[i].DTOriginalTypeInfo
			}
			err := r.add(result, &currDebeziumSchema.Fields[i], val, originalType, kind, currTableSchema.Columns()[i].DataType, currTableSchema.Columns()[i].PrimaryKey)
			if err != nil {
				return nil, xerrors.Errorf("unable to add column, err: %w", err)
			}
		} else {
			return nil, xerrors.Errorf("unable to get field %s from 'after'", currDebeziumSchema.Fields[i].Field)
		}
	}

	return result, nil
}

func NewReceiver(originalTypes map[abstract.TableID]map[string]*debeziumcommon.OriginalTypeInfo, schemaRegistryClient *confluent.SchemaRegistryClient) *Receiver {
	currUnpacker := unpacker.NewMessageUnpacker(schemaRegistryClient)
	var schemaFormat = debeziumparameters.ConverterApacheKafkaJSON
	if schemaRegistryClient != nil {
		schemaFormat = debeziumparameters.ConverterConfluentJSON
	}
	return &Receiver{
		originalTypes:         originalTypes,
		Unpacker:              currUnpacker,
		schemaFormat:          schemaFormat,
		tableSchemaCache:      make(map[string]tableSchemaCacheItem),
		tableSchemaCacheMutex: new(sync.RWMutex),
	}
}
