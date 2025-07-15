package protoparser

import (
	"fmt"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
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

type ProtoParser struct {
	metrics *stats.SourceStats

	cfg     ProtoParserConfig
	columns []string
	schemas *abstract.TableSchema

	includedSchemas   []abstract.ColSchema
	lbExtraSchemes    []abstract.ColSchema
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

func (p *ProtoParser) Do(msg parsers.Message, partition abstract.Partition) (res []abstract.ChangeItem) {
	iterSt := NewIterState(msg, partition)

	defer func() {
		if err := recover(); err != nil {
			res = []abstract.ChangeItem{
				unparsedChangeItem(iterSt, msg.Value, xerrors.Errorf("Do: panic recovered: %v", err)),
			}
		}
	}()

	sc, err := protoscanner.NewProtoScanner(p.cfg.ProtoScannerType, p.cfg.LineSplitter, msg.Value, p.cfg.ScannerMessageDesc)
	if err != nil {
		iterSt.IncrementCounter()
		res = append(res, unparsedChangeItem(iterSt, msg.Value, xerrors.Errorf("error creating scanner: %v", err)))
		return res
	}

	for sc.Scan() {
		// start with 1
		iterSt.IncrementCounter()

		protoMsg, err := sc.Message()
		if err != nil {
			res = append(res, unparsedChangeItem(iterSt, sc.RawData(), err))
			continue
		}

		changeItem := abstract.ChangeItem{
			ID:           0,
			LSN:          iterSt.Offset,
			CommitTime:   uint64(msg.WriteTime.UnixNano()),
			Counter:      iterSt.Counter(),
			Kind:         abstract.InsertKind,
			Schema:       "",
			Table:        tableName(iterSt.Partition),
			PartID:       "",
			ColumnNames:  p.columns,
			ColumnValues: nil,
			TableSchema:  p.schemas,
			OldKeys:      abstract.EmptyOldKeys(),
			TxID:         "",
			Query:        "",
			Size:         abstract.RawEventSize(uint64(sc.ApxDataLen())),
		}

		values, err := p.makeValues(iterSt, protoMsg)
		if err != nil {
			res = append(res, unparsedChangeItem(iterSt, sc.RawData(), err))
			continue
		}
		changeItem.ColumnValues = values

		res = append(res, changeItem)
	}

	if err := sc.Err(); err != nil {
		iterSt.IncrementCounter()
		res = append(res, unparsedChangeItem(iterSt, msg.Value, err))
	}

	return res
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
		Counter:     iterSt.Counter(),
		Schema:      "",
		PartID:      "",
		OldKeys:     abstract.EmptyOldKeys(),
		TxID:        "",
		Query:       "",
		Kind:        abstract.InsertKind,
		CommitTime:  uint64(iterSt.CreateTime.UnixNano()),
		Table:       fmt.Sprintf("%v_unparsed", tableName(iterSt.Partition)),
		ColumnNames: parsers.ErrParserColumns,
		TableSchema: parsers.ErrParserSchema,
		ColumnValues: []interface{}{
			iterSt.Partition.String(),
			iterSt.Offset,
			err.Error(),
			util.Sample(string(data), 1000),
		},
		Size: abstract.RawEventSize(uint64(len(data))),
	}
}

func (p *ProtoParser) makeValues(iterSt *iterState, protoMsg protoreflect.Message) ([]interface{}, error) {
	if iterSt == nil {
		return nil, xerrors.New("provided context is nil")
	}

	res := make([]interface{}, 0, len(p.columns))

	msgDesc := p.cfg.ProtoMessageDesc
	for _, sc := range p.includedSchemas {
		fd := msgDesc.Fields().ByTextName(sc.ColumnName)
		if fd == nil {
			return nil, xerrors.Errorf("can't find field descripter for field '%s'", sc.ColumnName)
		}

		if sc.Required && fd.HasPresence() && !protoMsg.Has(fd) {
			return nil, xerrors.Errorf("required field '%s' was not populated", sc.ColumnName)
		}

		val, err := extractValueRecursive(fd, protoMsg.Get(fd), maxEmbeddedStructDepth)
		if err != nil {
			return nil, xerrors.Errorf("error extracting value with column name '%s'", sc.ColumnName)
		}

		res = append(res, val)
	}

	for _, sc := range p.lbExtraSchemes {
		if !strings.HasPrefix(sc.ColumnName, parsers.SystemLbExtraColPrefix) {
			return nil, xerrors.Errorf("extra column field '%s' has no '%s' prefix", sc.ColumnName, parsers.SystemLbExtraColPrefix)
		}

		name := strings.TrimPrefix(sc.ColumnName, parsers.SystemLbExtraColPrefix)

		fd := msgDesc.Fields().ByTextName(name)
		if fd == nil {
			return nil, xerrors.Errorf("can't find field descriptor for field '%s'", name)
		}

		val, err := extractValueRecursive(fd, protoMsg.Get(fd), maxEmbeddedStructDepth)
		if err != nil {
			return nil, xerrors.Errorf("error extracting value with column name '%s'", sc.ColumnName)
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

func extractValueRecursive(fd protoreflect.FieldDescriptor, val protoreflect.Value, maxDepth int) (interface{}, error) {
	if maxDepth <= 0 {
		return nil, xerrors.Errorf("max recursion depth is reached")
	}

	if fd.IsList() {
		list := val.List()
		size := list.Len()
		items := make([]interface{}, size)

		for i := 0; i < size; i++ {
			val, err := extractValueExceptListRecursive(fd, list.Get(i), maxDepth-1)
			if err != nil {
				return nil, xerrors.New(err.Error())
			}

			items[i] = val
		}

		return items, nil
	}

	return extractValueExceptListRecursive(fd, val, maxDepth)
}

func extractValueExceptListRecursive(fd protoreflect.FieldDescriptor, val protoreflect.Value, maxDepth int) (interface{}, error) {
	if maxDepth <= 0 {
		return nil, xerrors.Errorf("max recursion depth is reached")
	}

	if fd.Kind() == protoreflect.MessageKind && !fd.IsMap() {
		items := make(map[string]interface{})
		msgDesc := fd.Message()
		msgVal := val.Message()

		for i := 0; i < msgDesc.Fields().Len(); i++ {
			fieldDesc := msgDesc.Fields().Get(i)
			val, err := extractValueRecursive(fieldDesc, msgVal.Get(fieldDesc), maxDepth-1)
			if err != nil {
				return nil, xerrors.New(err.Error())
			}

			items[fieldDesc.TextName()] = val
		}

		return items, nil
	}

	if fd.IsMap() {
		items := make(map[string]interface{})
		keyDesc := fd.MapKey()
		valDesc := fd.MapValue()

		var rangeError error
		val.Map().Range(func(mk protoreflect.MapKey, v protoreflect.Value) bool {
			keyVal, err := extractValueRecursive(keyDesc, mk.Value(), maxDepth-1)
			if err != nil {
				rangeError = err
				return false
			}

			valVal, err := extractValueRecursive(valDesc, v, maxDepth-1)
			if err != nil {
				rangeError = err
				return false
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

	return &ProtoParser{
		metrics:           metrics,
		cfg:               *cfg,
		schemas:           abstract.NewTableSchema(schemas),
		columns:           columns,
		includedSchemas:   schemas[:includedFieldsEndIdx],
		lbExtraSchemes:    schemas[includedFieldsEndIdx:lbExtraFieldsEndIdx],
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

func makeStringIndexMap(schemas []abstract.ColSchema) map[string]int {
	res := make(map[string]int)

	for i, sc := range schemas {
		res[sc.ColumnName] = i
	}

	return res
}
