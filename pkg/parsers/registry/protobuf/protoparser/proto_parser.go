package protoparser

import (
	"fmt"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/registry/protobuf/protoscanner"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/yt/go/schema"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	maxEmbeddedStructDepth = 100
)

type iterState struct {
	counter    int
	Partition  abstract.Partition
	Offset     uint64
	SeqNo      uint64
	CreateTime time.Time
	WriteTime  time.Time
}

func (st *iterState) IncrementCounter() {
	st.counter++
}

func (st *iterState) Counter() int {
	return st.counter
}

func NewIterState(msg parsers.Message, partition abstract.Partition) *iterState {
	return &iterState{
		counter:    0,
		Partition:  partition,
		Offset:     msg.Offset,
		SeqNo:      msg.SeqNo,
		CreateTime: msg.CreateTime,
		WriteTime:  msg.WriteTime,
	}
}

type schemaWithFieldDescriptor struct {
	schema abstract.ColSchema
	fd     protoreflect.FieldDescriptor
}

type ProtoParser struct {
	metrics *stats.SourceStats

	cfg     ProtoParserConfig
	columns []string
	schemas *abstract.TableSchema

	includedSchemas   []schemaWithFieldDescriptor
	lbExtraSchemas    []schemaWithFieldDescriptor
	auxFieldsIndexMap map[string]int
}

func (p *ProtoParser) ColSchema() []abstract.ColSchema {
	return p.schemas.Columns()
}

func (p *ProtoParser) ColumnNames() []string {
	return p.columns
}

func (p *ProtoParser) DoBatch(batch parsers.MessageBatch) (res []abstract.ChangeItem) {
	partition := abstract.NewPartition(batch.Topic, batch.Partition)

	for _, msg := range batch.Messages {
		res = append(res, p.Do(msg, partition)...)
	}

	return res
}

func (p *ProtoParser) do(msg parsers.Message, partition abstract.Partition) []abstract.ChangeItem {
	var result []abstract.ChangeItem

	iterSt := NewIterState(msg, partition)

	defer func() {
		if err := recover(); err != nil {
			result = []abstract.ChangeItem{
				unparsedChangeItem(iterSt, msg.Value, xerrors.Errorf("Do: panic recovered: %v", err)),
			}
		}
	}()

	sc, err := protoscanner.NewProtoScanner(p.cfg.ProtoScannerType, p.cfg.LineSplitter, msg.Value, p.cfg.ScannerMessageDesc)
	if err != nil {
		iterSt.IncrementCounter()
		result = append(result, unparsedChangeItem(iterSt, msg.Value, xerrors.Errorf("error creating scanner: %v", err)))
		return result
	}

	for sc.Scan() {
		result = append(result, p.processScanned(iterSt, sc, uint64(msg.WriteTime.UnixNano())))
	}

	return result
}

func (p *ProtoParser) processScanned(iterSt *iterState, sc protoscanner.ProtoScanner, commitTime uint64) abstract.ChangeItem {
	iterSt.IncrementCounter() // Starts with 1.

	protoMsg, err := sc.Message()
	if err != nil {
		return unparsedChangeItem(iterSt, sc.RawData(), err)
	}

	changeItem := abstract.ChangeItem{
		ID:               0,
		LSN:              iterSt.Offset,
		CommitTime:       commitTime,
		Counter:          iterSt.Counter(),
		Kind:             abstract.InsertKind,
		Schema:           "",
		Table:            tableName(iterSt.Partition),
		PartID:           "",
		ColumnNames:      p.columns,
		ColumnValues:     nil,
		TableSchema:      p.schemas,
		OldKeys:          abstract.EmptyOldKeys(),
		Size:             abstract.RawEventSize(uint64(sc.ApxDataLen())),
		TxID:             "",
		Query:            "",
		QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: "", PartitionNum: 0, Offset: 0, Index: 0},
	}

	values, err := p.makeValues(iterSt, protoMsg)
	if err != nil {
		return unparsedChangeItem(iterSt, sc.RawData(), err)
	}
	changeItem.ColumnValues = values
	return changeItem
}

func (p *ProtoParser) Do(msg parsers.Message, partition abstract.Partition) []abstract.ChangeItem {
	result := p.do(msg, partition)
	for i := range result {
		result[i].FillQueueMessageMeta(partition.Topic, int(partition.Partition), msg.Offset, i)
	}
	return result
}

func tableName(part abstract.Partition) string {
	result := part.Topic
	result = strings.ReplaceAll(result, "/", "_")
	result = strings.ReplaceAll(result, "@", "_")
	return result
}

func unparsedChangeItem(iterSt *iterState, data []byte, err error) abstract.ChangeItem {
	return abstract.ChangeItem{
		ID:          0,
		LSN:         iterSt.Offset,
		CommitTime:  uint64(iterSt.CreateTime.UnixNano()),
		Counter:     iterSt.Counter(),
		Kind:        abstract.InsertKind,
		Schema:      "",
		Table:       fmt.Sprintf("%v_unparsed", tableName(iterSt.Partition)),
		PartID:      "",
		ColumnNames: parsers.ErrParserColumns,
		ColumnValues: []interface{}{
			iterSt.Partition.String(),
			iterSt.Offset,
			err.Error(),
			util.Sample(string(data), 1000),
		},
		TableSchema:      parsers.ErrParserSchema,
		OldKeys:          abstract.EmptyOldKeys(),
		Size:             abstract.RawEventSize(uint64(len(data))),
		TxID:             "",
		Query:            "",
		QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: "", PartitionNum: 0, Offset: 0, Index: 0},
	}
}

func (p *ProtoParser) makeValues(iterSt *iterState, protoMsg protoreflect.Message) ([]interface{}, error) {
	if iterSt == nil {
		return nil, xerrors.New("provided context is nil")
	}

	res := make([]interface{}, 0, len(p.columns))

	for _, sc := range p.includedSchemas {
		if sc.schema.Required && sc.fd.HasPresence() && !protoMsg.Has(sc.fd) {
			return nil, xerrors.Errorf("required field '%s' was not populated", sc.schema.ColumnName)
		}

		if !protoMsg.Has(sc.fd) && p.cfg.NotFillEmptyFields {
			res = append(res, nil)
			continue
		}

		val, err := extractValueRecursive(sc.fd, protoMsg.Get(sc.fd), maxEmbeddedStructDepth, p.cfg.NotFillEmptyFields)
		if err != nil {
			return nil, xerrors.Errorf("error extracting value with column name '%s'", sc.schema.ColumnName)
		}

		res = append(res, val)
	}

	for _, sc := range p.lbExtraSchemas {
		val, err := extractValueRecursive(sc.fd, protoMsg.Get(sc.fd), maxEmbeddedStructDepth, p.cfg.NotFillEmptyFields)
		if err != nil {
			return nil, xerrors.Errorf("error extracting value with column name '%s'", sc.schema.ColumnName)
		}

		res = append(res, val)
	}

	auxRes := p.makeAuxValues(iterSt)
	res = append(res, auxRes...)

	if len(res) != len(p.columns) {
		return nil, xerrors.Errorf("logic error: len of values = %d, len of columns = %d - not equal", len(res), len(p.columns))
	}

	return res, nil
}

func extractValueRecursive(fd protoreflect.FieldDescriptor, val protoreflect.Value, maxDepth int, notFillEmptyFields bool) (interface{}, error) {
	if maxDepth <= 0 {
		return nil, xerrors.Errorf("max recursion depth is reached")
	}

	if fd.IsList() {
		list := val.List()
		size := list.Len()
		items := make([]interface{}, size)

		for i := 0; i < size; i++ {
			val, err := extractValueExceptListRecursive(fd, list.Get(i), maxDepth-1, notFillEmptyFields)
			if err != nil {
				return nil, xerrors.New(err.Error())
			}

			if val == nil && notFillEmptyFields {
				continue
			}

			items[i] = val
		}

		return items, nil
	}

	return extractValueExceptListRecursive(fd, val, maxDepth, notFillEmptyFields)
}

func extractValueExceptListRecursive(fd protoreflect.FieldDescriptor, val protoreflect.Value, maxDepth int, notFillEmptyFields bool) (interface{}, error) {
	if maxDepth <= 0 {
		return nil, xerrors.Errorf("max recursion depth is reached")
	}

	if fd.Kind() == protoreflect.MessageKind && !fd.IsMap() {
		items := make(map[string]interface{})
		msgDesc := fd.Message()
		msgVal := val.Message()

		cntEmpty := 0
		for i := 0; i < msgDesc.Fields().Len(); i++ {
			if !msgVal.Has(msgDesc.Fields().Get(i)) && notFillEmptyFields {
				cntEmpty++
				continue
			}
			fieldDesc := msgDesc.Fields().Get(i)
			val, err := extractValueRecursive(fieldDesc, msgVal.Get(fieldDesc), maxDepth-1, notFillEmptyFields)
			if err != nil {
				return nil, xerrors.New(err.Error())
			}

			items[fieldDesc.TextName()] = val
		}

		if cntEmpty == msgDesc.Fields().Len() {
			return nil, nil
		}

		return items, nil
	}

	if fd.IsMap() {
		items := make(map[string]interface{})
		keyDesc := fd.MapKey()
		valDesc := fd.MapValue()

		var rangeError error
		val.Map().Range(func(mk protoreflect.MapKey, v protoreflect.Value) bool {
			keyVal, err := extractValueRecursive(keyDesc, mk.Value(), maxDepth-1, notFillEmptyFields)
			if err != nil {
				rangeError = err
				return false
			}

			valVal, err := extractValueRecursive(valDesc, v, maxDepth-1, notFillEmptyFields)
			if err != nil {
				rangeError = err
				return false
			}

			if valVal == nil {
				return true
			}

			items[fmt.Sprint(keyVal)] = valVal
			return true
		})

		if rangeError != nil {
			return nil, xerrors.New(rangeError.Error())
		}

		return items, nil
	}

	return extractLeafValue(val)
}

func extractLeafValue(val protoreflect.Value) (interface{}, error) {
	rawVal := val.Interface()
	switch rawValTyped := rawVal.(type) {
	case protoreflect.EnumNumber:
		return int32(rawValTyped), nil
	default:
		return rawValTyped, nil
	}
}

func (p *ProtoParser) makeAuxValues(iterSt *iterState) []interface{} {
	res := make([]interface{}, len(p.auxFieldsIndexMap))
	if id, ok := p.auxFieldsIndexMap[parsers.SyntheticTimestampCol]; ok {
		res[id] = time.Time{} // TODO (or maybe drop)
	}
	if id, ok := p.auxFieldsIndexMap[parsers.SyntheticPartitionCol]; ok {
		res[id] = iterSt.Partition.String()
	}
	if id, ok := p.auxFieldsIndexMap[parsers.SyntheticOffsetCol]; ok {
		res[id] = iterSt.Offset
	}
	if id, ok := p.auxFieldsIndexMap[parsers.SyntheticIdxCol]; ok {
		res[id] = uint64(iterSt.Counter())
	}
	if id, ok := p.auxFieldsIndexMap[parsers.SystemLbCtimeCol]; ok {
		res[id] = iterSt.CreateTime
	}
	if id, ok := p.auxFieldsIndexMap[parsers.SystemLbWtimeCol]; ok {
		res[id] = iterSt.WriteTime
	}

	return res
}

func protoFieldDescToYtType(fd protoreflect.FieldDescriptor) schema.Type {
	if fd.IsList() || fd.IsMap() {
		return schema.TypeAny
	}

	switch fd.Kind() {
	case protoreflect.BoolKind:
		return schema.TypeBoolean
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind, protoreflect.EnumKind:
		return schema.TypeInt32
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return schema.TypeInt64
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return schema.TypeUint32
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return schema.TypeUint64
	case protoreflect.FloatKind:
		return schema.TypeFloat32
	case protoreflect.DoubleKind:
		return schema.TypeFloat64
	case protoreflect.StringKind:
		return schema.TypeString
	case protoreflect.BytesKind:
		return schema.TypeBytes
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return schema.TypeAny
	default:
		return schema.TypeAny
	}
}

func NewProtoParser(cfg *ProtoParserConfig, metrics *stats.SourceStats) (*ProtoParser, error) {
	if cfg == nil {
		return nil, xerrors.Errorf("nil config provided")
	}

	if metrics == nil {
		return nil, xerrors.Errorf("nil metrics provided")
	}

	colParamsMap := make(map[string]ColParams)
	for _, par := range cfg.IncludeColumns {
		if _, ok := colParamsMap[par.Name]; ok {
			return nil, xerrors.Errorf("duplicate column name '%s'", par.Name)
		}
		colParamsMap[par.Name] = par
	}

	var schemas []abstract.ColSchema
	msgDesc := cfg.ProtoMessageDesc
	keySet := make(map[string]bool)

	// Add keys to schemas
	for _, key := range cfg.PrimaryKeys {
		keySet[key] = true

		keyFieldDesc := msgDesc.Fields().ByTextName(key)
		if keyFieldDesc == nil {
			return nil, xerrors.Errorf("given key field '%s' not found in proto message desc", key)
		}

		cs := abstract.MakeTypedColSchema(keyFieldDesc.TextName(), string(protoFieldDescToYtType(keyFieldDesc)), true)
		cs.Required = !cfg.NullKeysAllowed

		params, ok := colParamsMap[key]
		if ok {
			if !cfg.NullKeysAllowed && !params.Required {
				return nil, xerrors.Errorf("key param '%s' must be set as required", key)
			}

			cs.Required = params.Required
		}

		schemas = append(schemas, cs)
	}

	// Add included fields to schemas except already added keys
	if len(cfg.IncludeColumns) == 0 {
		for i := 0; i < msgDesc.Fields().Len(); i++ {
			fieldDesc := msgDesc.Fields().Get(i)

			if keySet[fieldDesc.TextName()] {
				continue
			}

			cs := abstract.MakeTypedColSchema(fieldDesc.TextName(), string(protoFieldDescToYtType(fieldDesc)), false)
			schemas = append(schemas, cs)
		}
	} else {
		for _, params := range cfg.IncludeColumns {
			fieldDesc := msgDesc.Fields().ByTextName(params.Name)
			if fieldDesc == nil {
				return nil, xerrors.Errorf("requested column %s not found in proto descriptor", params.Name)
			}

			if keySet[fieldDesc.TextName()] {
				continue
			}

			cs := abstract.MakeTypedColSchema(fieldDesc.TextName(), string(protoFieldDescToYtType(fieldDesc)), false)
			cs.Required = params.Required

			schemas = append(schemas, cs)
		}
	}
	includedFieldsEndIdx := len(schemas)

	schemas = append(schemas, makeLbExtraSchemas(schemas, cfg, msgDesc)...)
	lbExtraFieldsEndIdx := len(schemas)

	auxSchemas := makeAuxSchemas(cfg)
	schemas = append(schemas, auxSchemas...)

	// schemas now: [included fields, lbExtra fields, aux fields]

	columns := make([]string, len(schemas))
	for i, val := range schemas {
		columns[i] = val.ColumnName
	}

	// save field descriptors for fast access without ambiguous name lookups
	includedSchemas, lbExtraSchemas, err := prepareFieldDescriptors(msgDesc, schemas, includedFieldsEndIdx, lbExtraFieldsEndIdx)
	if err != nil {
		return nil, err
	}

	return &ProtoParser{
		metrics:           metrics,
		cfg:               *cfg,
		schemas:           abstract.NewTableSchema(schemas),
		columns:           columns,
		includedSchemas:   includedSchemas,
		lbExtraSchemas:    lbExtraSchemas,
		auxFieldsIndexMap: makeStringIndexMap(auxSchemas),
	}, nil
}

func makeLbExtraSchemas(schemas []abstract.ColSchema, cfg *ProtoParserConfig, md protoreflect.MessageDescriptor) (res []abstract.ColSchema) {
	if !cfg.AddSystemColumns {
		return res
	}

	includeMap := make(map[string]bool)
	for _, cs := range schemas {
		includeMap[cs.ColumnName] = true
	}

	for i := 0; i < md.Fields().Len(); i++ {
		fieldDesc := md.Fields().Get(i)

		if includeMap[fieldDesc.TextName()] {
			continue
		}

		cs := abstract.MakeTypedColSchema(
			fmt.Sprintf("%s%s", parsers.SystemLbExtraColPrefix, fieldDesc.TextName()),
			string(protoFieldDescToYtType(fieldDesc)),
			false,
		)
		res = append(res, cs)
	}

	return res
}

func makeAuxSchemas(cfg *ProtoParserConfig) (res []abstract.ColSchema) {
	if cfg.TimeField != nil {
		res = append(
			res,
			abstract.MakeTypedColSchema(parsers.SyntheticTimestampCol, string(schema.TypeDatetime), true),
		)
	}

	if cfg.AddSyntheticKeys {
		res = append(
			res,
			abstract.MakeTypedColSchema(parsers.SyntheticPartitionCol, string(schema.TypeString), true),
			abstract.MakeTypedColSchema(parsers.SyntheticOffsetCol, string(schema.TypeUint64), true),
			abstract.MakeTypedColSchema(parsers.SyntheticIdxCol, string(schema.TypeUint64), true),
		)
	}

	if cfg.AddSystemColumns {
		res = append(
			res,
			abstract.MakeTypedColSchema(parsers.SystemLbCtimeCol, string(schema.TypeDatetime), !cfg.SkipDedupKeys),
			abstract.MakeTypedColSchema(parsers.SystemLbWtimeCol, string(schema.TypeDatetime), !cfg.SkipDedupKeys),
		)
	}

	for i := range res {
		if res[i].PrimaryKey {
			res[i].Required = !cfg.NullKeysAllowed
		}
	}

	return res
}

// prepareFieldDescriptors prepares field descriptors for included and lbExtra schemas to avoid ambiguous name lookups.
// It returns two slices of schemaWithFieldDescriptor: includedSchemas and lbExtraSchemas.
// includedSchemas contains field descriptors for included fields and their schemas.
// lbExtraSchemas contains field descriptors for lbExtra fields and their schemas.
// The order of the slices is the same as the order of the schemas in the input.
func prepareFieldDescriptors(
	msgDesc protoreflect.MessageDescriptor,
	schemas []abstract.ColSchema,
	includedFieldsEndIdx int,
	lbExtraFieldsEndIdx int,
) ([]schemaWithFieldDescriptor, []schemaWithFieldDescriptor, error) {
	includedSchemas := make([]schemaWithFieldDescriptor, 0, includedFieldsEndIdx)
	for i := 0; i < includedFieldsEndIdx; i++ {
		fd := msgDesc.Fields().ByTextName(schemas[i].ColumnName)
		if fd == nil {
			return nil, nil, xerrors.Errorf("can't find field descriptor for '%s'", schemas[i].ColumnName)
		}
		includedSchemas = append(includedSchemas, schemaWithFieldDescriptor{
			schema: schemas[i],
			fd:     fd,
		})
	}

	lbExtraSchemas := make([]schemaWithFieldDescriptor, 0, lbExtraFieldsEndIdx-includedFieldsEndIdx)
	for i := includedFieldsEndIdx; i < lbExtraFieldsEndIdx; i++ {
		if !strings.HasPrefix(schemas[i].ColumnName, parsers.SystemLbExtraColPrefix) {
			return nil, nil, xerrors.Errorf("extra column field '%s' has no '%s' prefix", schemas[i].ColumnName, parsers.SystemLbExtraColPrefix)
		}
		name := strings.TrimPrefix(schemas[i].ColumnName, parsers.SystemLbExtraColPrefix)
		fd := msgDesc.Fields().ByTextName(name)
		if fd == nil {
			return nil, nil, xerrors.Errorf("can't find field descriptor for '%s'", name)
		}
		lbExtraSchemas = append(lbExtraSchemas, schemaWithFieldDescriptor{
			schema: schemas[i],
			fd:     fd,
		})
	}

	return includedSchemas, lbExtraSchemas, nil
}

func makeStringIndexMap(schemas []abstract.ColSchema) map[string]int {
	res := make(map[string]int)

	for i, sc := range schemas {
		res[sc.ColumnName] = i
	}

	return res
}
