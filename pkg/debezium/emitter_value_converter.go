package debezium

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	debeziumcommon "github.com/transferria/transferria/pkg/debezium/common"
	"github.com/transferria/transferria/pkg/debezium/mysql"
	"github.com/transferria/transferria/pkg/debezium/packer"
	debeziumparameters "github.com/transferria/transferria/pkg/debezium/parameters"
	"github.com/transferria/transferria/pkg/debezium/pg"
	"github.com/transferria/transferria/pkg/debezium/typeutil"
	"github.com/transferria/transferria/pkg/debezium/ydb"
	"github.com/transferria/transferria/pkg/schemaregistry/format"
	"github.com/transferria/transferria/pkg/util"
	"github.com/transferria/transferria/tests/helpers/testsflag"
	"go.ytsaurus.tech/library/go/core/log"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

type Emitter struct {
	database             string // "db" in debezium payload - it's just field in "source". "database.dbname"
	databaseServerName   string // "name" in debezium payload - it's prefix for topic_name. "topic.prefix"
	connectorParameters  map[string]string
	version              string
	dropKeys             bool
	ignoreUnknownSources bool // set in 'true' only in tests!

	logger log.Logger

	keyPacker   packer.Packer
	valuePacker packer.Packer
}

var errUnknownSource = xerrors.New("unknown source type")

var opSchema = map[string]interface{}{
	"type":     "string",
	"optional": false,
	"field":    "op",
}

var tsMsSchema = map[string]interface{}{
	"type":     "int64",
	"optional": true,
	"field":    "ts_ms",
}

var transactionSchema = map[string]interface{}{
	"type": "struct",
	"fields": []map[string]interface{}{
		{
			"type":     "string",
			"optional": false,
			"field":    "id",
		}, {
			"type":     "int64",
			"optional": false,
			"field":    "total_order",
		}, {
			"type":     "int64",
			"optional": false,
			"field":    "data_collection_order",
		},
	},
	"optional": true,
	"field":    "transaction",
}

type emitType int

const (
	// It's when 1 changeItem generates 1 debezium event, for example:
	// - insert
	// - update (with updating non-pkey)

	regularEmitType = emitType(0)

	// It's when 1 changeItem generates >1 debezium events
	//
	// Update with changing pkey generates 3 debezium events:
	// - delete row with old pkey
	// - tombstone event
	// - insert row with new pkey
	//
	// Delete generates 2 debezium events:
	// - delete row
	// - tombstone event

	deleteEventEmitType    = emitType(1)
	tombstoneEventEmitType = emitType(2)
	insertEventEmitType    = emitType(3)
)

func arrColSchemaToFieldsDescr(arrColSchema []abstract.ColSchema, snapshot bool, connectorParameters map[string]string) (*fieldsDescr, error) {
	fields := newFields()
	for _, el := range arrColSchema {
		err := fields.AddFieldDescr(el, snapshot, connectorParameters)
		if err != nil {
			if debeziumcommon.IsUnknownTypeError(err) {
				unknownTypePolicy := connectorParameters[debeziumparameters.UnknownTypesPolicy]
				if unknownTypePolicy == debeziumparameters.UnknownTypesPolicySkip {
					continue
				} else if unknownTypePolicy == debeziumparameters.UnknownTypesPolicyToString {
					colSchemaCopy := el
					colSchemaCopy.DataType = ytschema.TypeString.String()
					colSchemaCopy.OriginalType = ""
					err := fields.AddFieldDescr(colSchemaCopy, snapshot, connectorParameters)
					if err != nil {
						return nil, xerrors.Errorf("unable to add field description of unknown type, colName: %s, err: %w", el.ColumnName, err)
					}
					continue
				}
			}
			return nil, xerrors.Errorf("unable to add field description: %w", err)
		}
	}
	return fields, nil
}

func arrColSchemaToFieldsDescrKeys(arrColSchema []abstract.ColSchema, snapshot bool, connectorParameters map[string]string) (*fieldsDescr, error) {
	fields := newFields()
	for _, el := range arrColSchema {
		if el.PrimaryKey {
			err := fields.AddFieldDescr(el, snapshot, connectorParameters)
			if err != nil {
				return nil, xerrors.Errorf("unable to add field description: %w", err)
			}
		}
	}
	return fields, nil
}

// add - function for adding (with conversion) one field
func add(colSchema *abstract.ColSchema, colName string, colVal interface{}, originalType string, ignoreUnknownSources, snapshot bool, connectorParameters map[string]string, result *debeziumcommon.Values) error {
	if strings.HasPrefix(originalType, "pg:") {
		if strings.HasSuffix(originalType, "[]") {
			if colVal == nil {
				result.AddVal(colName, nil)
				return nil
			}
			var colValArr []interface{}
			switch t := colVal.(type) {
			case []interface{}:
				colValArr = t
			case string:
				err := json.Unmarshal([]byte(t), &colValArr)
				if err != nil {
					return xerrors.Errorf("unable to unmarshal json arr, str: %s, err: %w", t, err)
				}
			}
			resultArrVal := make([]interface{}, 0, len(colValArr))
			for _, el := range colValArr {
				resultEl := debeziumcommon.NewValues(result.ConnectorParameters)
				err := pg.AddPg(resultEl, colSchema, colName, el, strings.TrimSuffix(originalType, "[]"), true, connectorParameters)
				if err != nil {
					return xerrors.Errorf("unable to convert pg event, err: %w", err)
				}
				resultArrVal = append(resultArrVal, resultEl.V[colName])
			}
			result.AddVal(colName, resultArrVal)
		} else {
			err := pg.AddPg(result, colSchema, colName, colVal, originalType, false, connectorParameters)
			if err != nil {
				return xerrors.Errorf("unable to convert pg event, err: %w", err)
			}
		}
	} else if strings.HasPrefix(originalType, "mysql:") {
		err := mysql.AddMysql(result, colName, colVal, originalType, snapshot, connectorParameters)
		if err != nil {
			return xerrors.Errorf("unable to convert mysql event, err: %w", err)
		}
	} else if strings.HasPrefix(originalType, "ydb:") {
		err := ydb.AddYDB(result, colName, colVal, originalType, connectorParameters)
		if err != nil {
			return xerrors.Errorf("unable to convert ydb event, err: %w", err)
		}
	} else {
		if ignoreUnknownSources {
			err := addCommon(result, colSchema, colVal)
			if err != nil {
				return xerrors.Errorf("unable to convert common event, err: %w", err)
			}
			return nil
		}
		return errUnknownSource
	}
	return nil
}

func makeValuesWithUnpack(tableSchema []abstract.ColSchema, connectorParameters map[string]string, names []string, vals []interface{}, keysOnly, ignoreUnknownSources, snapshot bool) (map[string]interface{}, error) {
	resultValues, err := makeValues(tableSchema, connectorParameters, names, vals, keysOnly, ignoreUnknownSources, snapshot)
	if err != nil {
		return nil, xerrors.Errorf("unable to make values, err: %w", err)
	}
	result := make(map[string]interface{})
	for k, v := range resultValues.V {
		result[k] = v
	}
	return result, nil
}

// makeValues - is used for build dict for both: 'after' and 'before'
func makeValues(tableSchema []abstract.ColSchema, connectorParameters map[string]string, names []string, vals []interface{}, keysOnly, ignoreUnknownSources, snapshot bool) (*debeziumcommon.Values, error) {
	mapColToIndex := make(map[string]int)
	for i, col := range tableSchema {
		mapColToIndex[col.ColumnName] = i
	}

	result := debeziumcommon.NewValues(connectorParameters)

	for i := range names {
		colName := names[i]
		colVal := vals[i]
		index, ok := mapColToIndex[colName]
		if !ok {
			return nil, xerrors.Errorf("invalid changeItem - column absent in schema: %s", colName)
		}
		if keysOnly && !tableSchema[index].PrimaryKey {
			continue
		}
		originalType := tableSchema[index].OriginalType
		err := add(&tableSchema[index], colName, colVal, originalType, ignoreUnknownSources, snapshot, connectorParameters, result)
		if err != nil {
			if debeziumcommon.IsUnknownTypeError(err) {
				unknownTypePolicy := connectorParameters[debeziumparameters.UnknownTypesPolicy]
				if unknownTypePolicy == debeziumparameters.UnknownTypesPolicySkip {
					continue
				} else if unknownTypePolicy == debeziumparameters.UnknownTypesPolicyToString {
					val, err := typeutil.UnknownTypeToString(colVal)
					if err != nil {
						return nil, xerrors.Errorf("unable to serialize unknown type to string, colName: %s, err: %w", colName, err)
					}
					result.AddVal(colName, val)
					continue
				}
			}
			return nil, xerrors.Errorf("unable to emit value, colName: %s, err: %w", colName, err)
		}
	}
	return result, nil
}

func (m *Emitter) MaxMessageSize() int {
	return debeziumparameters.GetBatchingMaxSize(m.connectorParameters)
}

func (m *Emitter) GetPackers() (packer.Packer, packer.Packer) {
	return m.keyPacker, m.valuePacker
}

// makeKey - builds 'key' for kafka
// for inserts/updates(without pkey changing) - just pkeys from 'after'
// for deletes/update(with pkey changing) - extracts pkeys from OldKeys
func (m *Emitter) makeKey(changeItem *abstract.ChangeItem, useAfter bool, ignoreUnknownSources, snapshot bool) (*debeziumcommon.Values, error) {
	if useAfter || changeItem.OldKeys.KeyNames == nil {
		return buildKV(changeItem, m.connectorParameters, true, ignoreUnknownSources, snapshot)
	}

	mapColToIndex := make(map[string]int)
	for i, col := range changeItem.TableSchema.Columns() {
		mapColToIndex[col.ColumnName] = i
	}

	result, err := makeValues(changeItem.TableSchema.Columns(), m.connectorParameters, changeItem.OldKeys.KeyNames, changeItem.OldKeys.KeyValues, true, ignoreUnknownSources, snapshot)
	if err != nil {
		return nil, xerrors.Errorf("unable to make key: %w", err)
	}
	return result, nil
}

// hasPreviousValues - happens when replica_identity=full (for pg), and same things for other databases
func hasPreviousValues(changeItem *abstract.ChangeItem) bool {
	pkeysNum := 0
	for _, el := range changeItem.TableSchema.Columns() {
		if el.PrimaryKey {
			pkeysNum++
		}
	}
	return len(changeItem.OldKeys.KeyNames) > pkeysNum
}

// BuildKVMap - builds 'after' k-v map
func BuildKVMap(changeItem *abstract.ChangeItem, connectorParameters map[string]string, snapshot bool) (map[string]interface{}, error) {
	values, err := buildKV(changeItem, connectorParameters, false, true, snapshot)
	if err != nil {
		return nil, xerrors.Errorf("unable to build kv: %w", err)
	}
	return values.V, nil
}

// buildKV - builds 'after' k-v map
func buildKV(changeItem *abstract.ChangeItem, connectorParameters map[string]string, keysOnly, ignoreUnknownSources, snapshot bool) (*debeziumcommon.Values, error) {
	mapColNameToIndex := make(map[string]int)
	for i, col := range changeItem.TableSchema.Columns() {
		mapColNameToIndex[col.ColumnName] = i
	}

	result, err := makeValues(changeItem.TableSchema.Columns(), connectorParameters, changeItem.ColumnNames, changeItem.ColumnValues, keysOnly, ignoreUnknownSources, snapshot)
	if err != nil {
		return nil, xerrors.Errorf("unable to emit value: %w", err)
	}

	if keysOnly {
		return result, nil
	}

	// handle TOAST
	if len(changeItem.TableSchema.Columns()) > len(changeItem.ColumnNames) {
		notToastedColumns := make(map[string]bool)
		for _, el := range changeItem.ColumnNames {
			notToastedColumns[el] = true
		}
		for _, currColumn := range changeItem.TableSchema.Columns() {
			if _, ok := notToastedColumns[currColumn.ColumnName]; !ok {
				// TOASTed column
				result.AddVal(currColumn.ColumnName, connectorParameters[debeziumparameters.UnavailableValuePlaceholder])
			}
		}
	}

	return result, nil
}

func (m *Emitter) buildSource(changeItem *abstract.ChangeItem, snapshot bool) map[string]interface{} {
	var snapshotVal string
	if snapshot {
		snapshotVal = "true"
	} else {
		snapshotVal = "false"
	}

	result := map[string]interface{}{
		"version":  m.version,
		"name":     m.databaseServerName,
		"ts_ms":    changeItem.CommitTime / 1000000,
		"snapshot": snapshotVal,
		"db":       m.database,
		"table":    changeItem.Table,
	}

	switch debeziumparameters.GetSourceType(m.connectorParameters) {
	case debeziumparameters.SourceTypePg:
		result["db"] = m.database
		result["connector"] = "postgresql"
		result["lsn"] = changeItem.LSN
		result["schema"] = changeItem.Schema
		result["txId"] = changeItem.ID
		result["xmin"] = nil
	case debeziumparameters.SourceTypeMysql:
		file, pos := typeutil.LSNToFileAndPos(changeItem.LSN)
		result["db"] = changeItem.Schema
		result["connector"] = "mysql"
		result["file"] = file
		result["pos"] = pos
		var gtid *string = nil
		if changeItem.TxID != "" {
			gtid = &changeItem.TxID
		}
		result["gtid"] = gtid
		result["query"] = nil
		result["row"] = 0
		result["server_id"] = 0
		result["thread"] = nil
	default:
	}
	return result
}

func (m *Emitter) ToKafkaSchemaKey(changeItem *abstract.ChangeItem, snapshot bool) ([]byte, error) {
	fieldsKey, err := arrColSchemaToFieldsDescrKeys(changeItem.TableSchema.Columns(), snapshot, m.connectorParameters)
	if err != nil {
		return nil, xerrors.Errorf("unable to get field keys: %w", err)
	}
	resultMap := map[string]interface{}{
		"fields":   fieldsKey.V,
		"name":     fmt.Sprintf("%s.%s.%s.Key", m.databaseServerName, changeItem.Schema, changeItem.Table),
		"optional": false,
		"type":     "struct",
	}
	result, err := util.JSONMarshalUnescape(resultMap)
	if err != nil {
		return nil, xerrors.Errorf("unable to marshal kafka_schema key, err: %w", err)
	}
	return result, nil
}

// ToKafkaPayloadKey - generate schema for a key message
func (m *Emitter) ToKafkaPayloadKey(changeItem *abstract.ChangeItem, snapshot bool, emitType emitType) ([]byte, error) {
	afterKey, err := m.makeKey(changeItem, emitType == insertEventEmitType, m.ignoreUnknownSources, snapshot)
	if err != nil {
		return nil, xerrors.Errorf("unable to make key payload: %w", err)
	}
	return util.JSONMarshalUnescape(afterKey.V)
}

// ToKafkaSchemaVal - generate a schema for a val message
func (m *Emitter) ToKafkaSchemaVal(changeItem *abstract.ChangeItem, snapshot bool) ([]byte, error) {
	fieldsVal, err := arrColSchemaToFieldsDescr(changeItem.TableSchema.Columns(), snapshot, m.connectorParameters)
	if err != nil {
		return nil, xerrors.Errorf("unable to make fields description: %w", err)
	}

	beforeSchema := make(map[string]interface{})
	beforeSchema["type"] = "struct"
	beforeSchema["fields"] = fieldsVal.V
	beforeSchema["optional"] = true
	beforeSchema["name"] = fmt.Sprintf("%s.%s.%s.Value", m.databaseServerName, changeItem.Schema, changeItem.Table)
	beforeSchema["field"] = "before"

	afterSchema := make(map[string]interface{})
	afterSchema["type"] = "struct"
	afterSchema["fields"] = fieldsVal.V
	afterSchema["optional"] = true
	afterSchema["name"] = fmt.Sprintf("%s.%s.%s.Value", m.databaseServerName, changeItem.Schema, changeItem.Table)
	afterSchema["field"] = "after"

	schemaFields := make([]interface{}, 0)
	schemaFields = append(schemaFields, beforeSchema)
	schemaFields = append(schemaFields, afterSchema)
	schemaFields = append(schemaFields, buildSourceSchemaDescr(debeziumparameters.GetSourceType(m.connectorParameters)))
	schemaFields = append(schemaFields, opSchema)
	schemaFields = append(schemaFields, tsMsSchema)
	schemaFields = append(schemaFields, transactionSchema)

	resultMap := map[string]interface{}{
		"type":     "struct",
		"fields":   schemaFields,
		"optional": false,
		"name":     fmt.Sprintf("%s.%s.%s.Envelope", m.databaseServerName, changeItem.Schema, changeItem.Table),
	}

	result, err := util.JSONMarshalUnescape(resultMap)
	if err != nil {
		return nil, xerrors.Errorf("unable to marshal ToKafkaSchemaVal, err: %w", err)
	}
	return result, nil
}

func (m *Emitter) ToKafkaPayloadVal(changeItem *abstract.ChangeItem, payloadTSMS time.Time, snapshot bool, emitType emitType) ([]byte, error) {
	payloadObj, err := m.valPayload(changeItem, payloadTSMS, snapshot, emitType)
	if err != nil {
		return nil, xerrors.Errorf("unable to make key payload: %w", err)
	}
	return util.JSONMarshalUnescape(payloadObj)
}

// valPayload - generate a payload for a val message
func (m *Emitter) valPayload(changeItem *abstract.ChangeItem, payloadTSMS time.Time, snapshot bool, emitType emitType) (map[string]interface{}, error) {
	op, err := kindToOp(changeItem.Kind, snapshot, emitType)
	if err != nil {
		return nil, xerrors.Errorf("unsupported kind: %w", err)
	}
	var after, before map[string]interface{}
	if op == "d" {
		after = nil
		before = make(map[string]interface{})
		for _, el := range changeItem.TableSchema.Columns() {
			before[el.ColumnName] = nil
		}

		if debeziumparameters.GetSourceType(m.connectorParameters) == debeziumparameters.SourceTypeMysql {
			result, err := makeValues(changeItem.TableSchema.Columns(), m.connectorParameters, changeItem.ColumnNames, changeItem.ColumnValues, false, m.ignoreUnknownSources, snapshot)
			if err != nil {
				return nil, xerrors.Errorf("unable to emit value: %w", err)
			}
			for k, v := range result.V {
				before[k] = v
			}
		}

		beforeOverride, err := makeValuesWithUnpack(changeItem.TableSchema.Columns(), m.connectorParameters, changeItem.OldKeys.KeyNames, changeItem.OldKeys.KeyValues, false, m.ignoreUnknownSources, snapshot)
		if err != nil {
			return nil, xerrors.Errorf("unable to build 'before' values, err: %w", err)
		}
		for k, v := range beforeOverride {
			before[k] = v
		}
	} else {
		afterVals, err := buildKV(changeItem, m.connectorParameters, false, m.ignoreUnknownSources, snapshot)
		if err != nil {
			return nil, xerrors.Errorf("unable to build key-value: %w", err)
		}

		after = afterVals.V
		if op == "u" && hasPreviousValues(changeItem) { // see description in TM-5250 (vanilla debezium in case of: pg with REPLICA_IDENTITY=DEFAULT - doesn't contain pkeys in 'before')
			before, err = makeValuesWithUnpack(changeItem.TableSchema.Columns(), m.connectorParameters, changeItem.OldKeys.KeyNames, changeItem.OldKeys.KeyValues, false, m.ignoreUnknownSources, snapshot)
			if err != nil {
				return nil, xerrors.Errorf("unable to build 'before' values, err: %w", err)
			}
		} else {
			before = nil
		}
	}
	tsMs := int64(0)
	if !payloadTSMS.IsZero() {
		tsMs = payloadTSMS.UnixNano() / 1000000
	}

	return map[string]interface{}{
		"before":      before,
		"after":       after,
		"source":      m.buildSource(changeItem, snapshot),
		"op":          op,
		"ts_ms":       tsMs,
		"transaction": nil,
	}, nil
}

func (m *Emitter) toConfluentSchema(schemaArr []byte, makeClosedContentModel bool) ([]byte, error) {
	kafkaSchema, err := format.KafkaJSONSchemaFromArr(schemaArr)
	if err != nil {
		return nil, xerrors.Errorf("can't convert map into kafka json schema: %w", err)
	}
	rawSchema, err := util.JSONMarshalUnescape(kafkaSchema.ToConfluentSchema(makeClosedContentModel))
	if err != nil {
		return nil, xerrors.Errorf("unable to marshal schema in confluent json format: %w", err)
	}
	return rawSchema, nil
}

func (m *Emitter) ToConfluentSchemaKey(changeItem *abstract.ChangeItem, snapshot bool) ([]byte, error) {
	keySchema, err := m.ToKafkaSchemaKey(changeItem, snapshot)
	if err != nil {
		return nil, xerrors.Errorf("can't build key schema: %w", err)
	}
	result, err := m.toConfluentSchema(keySchema, debeziumparameters.GetKeyConverterDTJSONGenerateClosedContentSchema(m.connectorParameters))
	if err != nil {
		return nil, xerrors.Errorf("can't convert key schema into confluent: %w", err)
	}
	return result, nil
}

func (m *Emitter) ToConfluentSchemaVal(changeItem *abstract.ChangeItem, snapshot bool) ([]byte, error) {
	valSchema, err := m.ToKafkaSchemaVal(changeItem, snapshot)
	if err != nil {
		return nil, xerrors.Errorf("can't build val schema: %w", err)
	}
	result, err := m.toConfluentSchema(valSchema, debeziumparameters.GetValueConverterDTJSONGenerateClosedContentSchema(m.connectorParameters))
	if err != nil {
		return nil, xerrors.Errorf("can't convert val schema into confluent: %w", err)
	}
	return result, nil
}

func (m *Emitter) skipTombstoneEvent() bool {
	return debeziumparameters.GetTombstonesOnDelete(m.connectorParameters) == debeziumparameters.BoolFalse
}

// emitOneDebeziumMessage - function which emit one debezium message, it is called only from EmitKV,
// EmitKV can emit for one changeItem different amount of messages - from 0 to 3
// When EmitKV wants to emit N messages for one changeItem, it calls N times function emitOneDebeziumMessage, every time with different emitType argument
// Cases, when changeItem derives different amount of messages:
//
//	    0 - for not insert/update/delete
//	    1 - for insert & for typical update (not changing pkey)
//		       emitType: regularEmitType
//	    2 - for delete
//	        emitType: deleteEventEmitType, tombstoneEventEmitType
//	    3 - for update, changing pkey: delete, tombstone, insert
//	        emitType: deleteEventEmitType, tombstoneEventEmitType, insertEventEmitType
func (m *Emitter) emitOneDebeziumMessage(
	changeItem *abstract.ChangeItem,
	payloadTSMS time.Time,
	snapshot bool,
	emitType emitType,
	sessionPackers packer.SessionPackers,
) (string, *string, error) {
	// --------------------------------------
	// build result 'key'
	// In kafka one debezium message consist of key & val (for the tombstone val==null)
	// So, this is the 'key' building

	var key []byte = nil
	if !m.dropKeys {
		var err error
		key, err = sessionPackers.Packer(true).Pack(
			changeItem,
			func(changeItem *abstract.ChangeItem) ([]byte, error) {
				return m.ToKafkaPayloadKey(changeItem, snapshot, emitType)
			},
			func(changeItem *abstract.ChangeItem) ([]byte, error) {
				return m.ToKafkaSchemaKey(changeItem, snapshot)
			},
			nil,
		)
		if err != nil {
			return "", nil, xerrors.Errorf("unable to pack key, err: %w", err)
		}
	}

	// --------------------------------------

	if emitType == tombstoneEventEmitType {
		return string(key), nil, nil
	}

	// --------------------------------------
	// build result 'val'
	// In kafka one debezium message consist of key & val (for the tombstone val==null)
	// So, this is the 'val' building

	val, err := sessionPackers.Packer(false).Pack(
		changeItem,
		func(changeItem *abstract.ChangeItem) ([]byte, error) {
			return m.ToKafkaPayloadVal(changeItem, payloadTSMS, snapshot, emitType)
		},
		func(changeItem *abstract.ChangeItem) ([]byte, error) {
			return m.ToKafkaSchemaVal(changeItem, snapshot)
		},
		nil,
	)
	if err != nil {
		return "", nil, xerrors.Errorf("unable to pack val, err: %w", err)
	}

	valStr := string(val)
	return string(key), &valStr, nil
}

// EmitKV - main exported method - generates kafka key & kafka value
func (m *Emitter) emitKV(changeItem *abstract.ChangeItem, payloadTSMS time.Time, snapshot bool, sessionPackers packer.SessionPackers) ([]debeziumcommon.KeyValue, error) {
	if changeItem.Kind != abstract.InsertKind && changeItem.Kind != abstract.UpdateKind && changeItem.Kind != abstract.DeleteKind {
		return []debeziumcommon.KeyValue{}, nil
	}

	if sessionPackers == nil {
		sessionPackers = packer.NewDefaultSessionPackers(m.keyPacker, m.valuePacker)
	}

	if changeItem.KeysChanged() {
		key0, val0, err := m.emitOneDebeziumMessage(changeItem, payloadTSMS, snapshot, deleteEventEmitType, sessionPackers)
		if err != nil {
			return nil, xerrors.Errorf("unable to emit debezium event part 0: %w", err)
		}
		key1, val1, err := m.emitOneDebeziumMessage(changeItem, payloadTSMS, snapshot, tombstoneEventEmitType, sessionPackers)
		if err != nil {
			return nil, xerrors.Errorf("unable to emit debezium event part 1: %w", err)
		}
		key2, val2, err := m.emitOneDebeziumMessage(changeItem, payloadTSMS, snapshot, insertEventEmitType, sessionPackers)
		if err != nil {
			return nil, xerrors.Errorf("unable to emit debezium event part 2: %w", err)
		}
		if m.skipTombstoneEvent() {
			return []debeziumcommon.KeyValue{{DebeziumKey: key0, DebeziumVal: val0}, {DebeziumKey: key2, DebeziumVal: val2}}, nil
		}
		return []debeziumcommon.KeyValue{{DebeziumKey: key0, DebeziumVal: val0}, {DebeziumKey: key1, DebeziumVal: val1}, {DebeziumKey: key2, DebeziumVal: val2}}, nil
	}

	if changeItem.Kind == abstract.DeleteKind {
		key0, val0, err := m.emitOneDebeziumMessage(changeItem, payloadTSMS, snapshot, deleteEventEmitType, sessionPackers)
		if err != nil {
			return nil, xerrors.Errorf("unable to emit debezium event part 0: %w", err)
		}
		if m.skipTombstoneEvent() {
			return []debeziumcommon.KeyValue{{DebeziumKey: key0, DebeziumVal: val0}}, nil
		}
		key1, val1, err := m.emitOneDebeziumMessage(changeItem, payloadTSMS, snapshot, tombstoneEventEmitType, sessionPackers)
		if err != nil {
			return nil, xerrors.Errorf("unable to emit debezium event part 1: %w", err)
		}
		return []debeziumcommon.KeyValue{{DebeziumKey: key0, DebeziumVal: val0}, {DebeziumKey: key1, DebeziumVal: val1}}, nil
	}

	key, val, err := m.emitOneDebeziumMessage(changeItem, payloadTSMS, snapshot, regularEmitType, sessionPackers)
	if err != nil {
		return nil, xerrors.Errorf("unable to emit debezium event: %w", err)
	}
	return []debeziumcommon.KeyValue{{DebeziumKey: key, DebeziumVal: val}}, nil
}

// EmitKV - main exported method - generates kafka key & kafka value
func (m *Emitter) EmitKV(changeItem *abstract.ChangeItem, payloadTSMS time.Time, snapshot bool, sessionPackers packer.SessionPackers) ([]debeziumcommon.KeyValue, error) {
	result, err := m.emitKV(changeItem, payloadTSMS, snapshot, sessionPackers)
	if err != nil {
		return nil, xerrors.Errorf("unable to emitKV, err: %w", err)
	}
	if testsflag.IsTest() { // in tests add extra validation
		m.logger.Info("EXTRA_VALIDATION_ON_TESTS__DEBEZIUM CALLED")
		if m.ignoreUnknownSources {
			m.logger.Info("EXTRA VALIDATION SKIPPED BCS OF 'ignoreUnknownSources'")
		} else {
			validateOnTests(m.connectorParameters, changeItem)
		}
	}
	return result, nil
}

func (m *Emitter) TestSetIgnoreUnknownSources(ignoreUnknownSources bool) {
	m.ignoreUnknownSources = ignoreUnknownSources
}

func GetPayloadTSMS(changeItem *abstract.ChangeItem) time.Time {
	return time.Unix(int64(changeItem.CommitTime/1000000000), int64(changeItem.CommitTime%1000000000))
}

func NewMessagesEmitter(connectorParameters map[string]string, version string, dropKeys bool, logger log.Logger) (*Emitter, error) {
	keyPacker, err := packer.NewKeyPackerFromDebeziumParameters(connectorParameters, logger)
	if err != nil {
		return nil, xerrors.Errorf("can't create key message processor: %w", err)
	}
	valuePacker, err := packer.NewValuePackerFromDebeziumParameters(connectorParameters, logger)
	if err != nil {
		return nil, xerrors.Errorf("can't create value message processor: %w", err)
	}
	return &Emitter{
		database:             debeziumparameters.GetDBName(connectorParameters),
		databaseServerName:   debeziumparameters.GetTopicPrefix(connectorParameters),
		connectorParameters:  debeziumparameters.EnrichedWithDefaults(connectorParameters),
		version:              version,
		ignoreUnknownSources: false,
		keyPacker:            keyPacker,
		logger:               logger,
		valuePacker:          valuePacker,
		dropKeys:             dropKeys,
	}, nil
}
