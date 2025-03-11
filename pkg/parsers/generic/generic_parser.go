package generic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	"github.com/goccy/go-json"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/format"
	"github.com/transferria/transferria/pkg/parsers"
	"github.com/transferria/transferria/pkg/parsers/registry/logfeller/lib"
	"github.com/transferria/transferria/pkg/stats"
	"github.com/transferria/transferria/pkg/util/castx"
	"github.com/valyala/fastjson"
	"github.com/valyala/fastjson/fastfloat"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yson"
	"golang.org/x/sync/semaphore"
)

type ParserConfig interface {
	isParserConfig()
}

const ColNameTimestamp = "_timestamp"
const ColNamePartition = "_partition"
const ColNameOffset = "_offset"
const ColNameIdx = "_idx"

type AuxParserOpts struct {
	Topic string

	// if true - add into schema (and add value) fields _timestamp/_partition/_offset/_idx
	// firstly needed only for cloudLogging parser - bcs these fields is unwanted there, and there was no way turn it off
	AddDedupeKeys bool
	// if true - if there are no pkey in changeItem - mark _timestamp/_partition/_offset/_idx as 'system' key
	MarkDedupeKeysAsSystem bool

	// if true - add into schema fields: _topic
	// if true - add into values: _lb_ctime, _lb_wtime, _lb_extra_%v
	AddSystemColumns bool

	// if true - fill value from 'msg.Headers["logtype"]'
	AddTopicColumn bool

	// if true - add into schema (and add value) field '_rest'
	AddRest bool

	TimeField     *abstract.TimestampCol
	InferTimeZone bool

	NullKeysAllowed   bool
	DropUnparsed      bool
	MaskSecrets       bool
	IgnoreColumnPaths bool
	TableSplitter     *abstract.TableSplitter
	Sniff             bool

	// if true  - use json.Number to parse numbers in any
	// if false - use float64 to parse numbers in any
	UseNumbersInAny bool

	// if true - unescape string values:
	//		for tskv - unescape special symbols in string values
	//		for json - use repeatedly json.Unmarshal(val) for strings if it possible
	UnescapeStringValues bool

	// UnpackBytesBase64 will try to parse `bytes`-typed column as base64 encoded byte array
	UnpackBytesBase64 bool
}

type LogfellerParserConfig struct {
	ParserName       string
	SplitterName     string
	OverriddenFields []abstract.ColSchema

	AuxOpts AuxParserOpts
}

func (c *LogfellerParserConfig) isParserConfig() {}

func dedupColumnName(expected string, cols []abstract.ColSchema) string {
	for _, col := range cols {
		if col.ColumnName == expected {
			return dedupColumnName(fmt.Sprintf("_delivery_%v", expected), cols)
		}
	}
	return expected
}

func newColSchema(columnName string, dataType schema.Type, isPrimaryKey bool) abstract.ColSchema {
	colSchema := abstract.NewColSchema(
		columnName,
		dataType,
		isPrimaryKey,
	)
	if isPrimaryKey {
		colSchema.Required = true
	}
	return colSchema
}

func addAuxFields(baseFields []abstract.ColSchema, opts AuxParserOpts) []abstract.ColSchema {
	result := baseFields

	if opts.AddSystemColumns {
		result = append(result, abstract.NewColSchema(
			dedupColumnName("_topic", result),
			schema.TypeBytes,
			false,
		))
	}
	if opts.AddRest {
		result = append(result, abstract.NewColSchema(
			dedupColumnName("_rest", result),
			schema.TypeAny,
			false,
		))
	}
	if opts.AddDedupeKeys {
		skipSystemKeys := false
		if opts.MarkDedupeKeysAsSystem {
			for _, f := range baseFields {
				if f.PrimaryKey {
					skipSystemKeys = true
					break
				}
			}
		}
		result = append(result, newColSchema(
			dedupColumnName(ColNameTimestamp, result),
			schema.TypeTimestamp,
			!skipSystemKeys,
		))
		result = append(result, newColSchema(
			dedupColumnName(ColNamePartition, result),
			schema.TypeBytes,
			!skipSystemKeys,
		))
		result = append(result, newColSchema(
			dedupColumnName(ColNameOffset, result),
			schema.TypeUint64,
			!skipSystemKeys,
		))
		result = append(result, newColSchema(
			dedupColumnName(ColNameIdx, result),
			schema.TypeUint32,
			!skipSystemKeys,
		))
	}
	return result
}

func auxFieldsToIndex(baseFields, finalSchema []abstract.ColSchema) map[string]int {
	result := make(map[string]int)
	for i := len(baseFields); i < len(finalSchema); i++ {
		result[finalSchema[i].ColumnName] = i
	}
	return result
}

type GenericParserConfig struct {
	Format             string
	SchemaResourceName string
	Fields             []abstract.ColSchema

	AuxOpts AuxParserOpts
}

func (c *GenericParserConfig) isParserConfig() {}

//---------------------------------------------------------------------------------------------------------------------

type GenericParser struct {
	logger  log.Logger
	metrics *stats.SourceStats

	lfParser   bool
	lfCfg      *LogfellerParserConfig
	genericCfg *GenericParserConfig
	auxOpts    *AuxParserOpts

	rawFields []abstract.ColSchema
	known     map[string]bool
	schema    *abstract.TableSchema

	auxFieldsToIndex map[string]int

	columns    []string
	colTypeMap map[string]string

	name           string
	jsonParserPool fastjson.ParserPool
}

type lfResult struct {
	Error   string `yson:"error"`
	RawLine string `yson:"raw"`

	ParsedRecord map[string]interface{} `yson:"parsed"`
}

type lfResultBatch []lfResult

var (
	UnparsedCols = cols(UnparsedSchema.Columns())
)

func cols(blankSchema []abstract.ColSchema) []string {
	res := make([]string, len(blankSchema))
	for i := range blankSchema {
		res[i] = blankSchema[i].ColumnName
	}
	return res
}

var (
	UnparsedSchema = abstract.NewTableSchema([]abstract.ColSchema{
		{
			ColumnName: ColNameTimestamp,
			DataType:   schema.TypeTimestamp.String(),
			PrimaryKey: true,
			Required:   true,
		},
		{
			ColumnName: ColNamePartition,
			DataType:   schema.TypeBytes.String(),
			PrimaryKey: true,
			Required:   true,
		},
		{
			ColumnName: ColNameOffset,
			DataType:   schema.TypeUint64.String(),
			PrimaryKey: true,
			Required:   true,
		},
		{
			ColumnName: ColNameIdx,
			DataType:   schema.TypeUint32.String(),
			PrimaryKey: true,
			Required:   true,
		},
		{
			ColumnName: "unparsed_row",
			DataType:   schema.TypeBytes.String(),
		},
		{
			ColumnName: "reason",
			DataType:   schema.TypeBytes.String(),
		},
	})
)

func IsGenericUnparsedSchema(schema *abstract.TableSchema) bool {
	originalColumns := schema.Columns()
	unparsedColumns := UnparsedSchema.Columns()
	if len(originalColumns) != len(unparsedColumns) {
		return false
	}
	for i := 0; i < len(originalColumns); i++ {
		if originalColumns[i].ColumnName != unparsedColumns[i].ColumnName ||
			// type check contradicts timestamp hacks: https://github.com/transferria/transferria/arcadia/transfer_manager/go/pkg/providers/yt/sink/sink.go?rev=r13620609#L1018
			// originalColumns[i].DataType != unparsedColumns[i].DataType ||
			originalColumns[i].PrimaryKey != unparsedColumns[i].PrimaryKey ||
			originalColumns[i].Required != unparsedColumns[i].Required {
			return false
		}
	}
	return true
}

func (lfr *lfResult) IsUnparsed() bool {
	return lfr.Error != ""
}

func newLfResult(raw []byte) ([]lfResult, error) {
	var result lfResultBatch
	if err := yson.Unmarshal(raw, &result); err != nil {
		return nil, xerrors.Errorf("unable to unmarshal logfeller result: %w", err)
	}
	return result, nil
}

func (p *GenericParser) makeChangeItem(item map[string]interface{}, idx int, line string, partition abstract.Partition, msg parsers.Message) abstract.ChangeItem {
	changeItem := abstract.ChangeItem{
		ID:           0,
		LSN:          msg.Offset,
		CommitTime:   uint64(msg.WriteTime.UnixNano()),
		Counter:      0,
		Kind:         abstract.InsertKind,
		Schema:       "",
		Table:        "",
		PartID:       partition.String(),
		ColumnNames:  p.columns,
		ColumnValues: make([]interface{}, len(p.columns)),
		TableSchema:  p.schema,
		OldKeys:      abstract.OldKeysType{KeyNames: nil, KeyTypes: nil, KeyValues: nil},
		TxID:         "",
		Query:        "",
		Size:         abstract.RawEventSize(uint64(len(line))),
	}
	if p.auxOpts.AddSystemColumns {
		item["_lb_ctime"] = msg.CreateTime
		item["_lb_wtime"] = msg.WriteTime
		for k, v := range msg.Headers {
			item[fmt.Sprintf("_lb_extra_%v", k)] = v
		}
	}
	for fIdx, key := range p.rawFields {
		if key.IsNestedKey() {
			if v, err := lookupComplex(item, key.Path); err != nil {
				if p.auxOpts.NullKeysAllowed {
					continue
				}
				if p.schema.Columns()[fIdx].PrimaryKey || p.schema.Columns()[fIdx].Required {
					return p.newUnparsed(partition, line, fmt.Sprintf("lookupComplex error %v: %v", key.ColumnName, err.Error()), idx, msg)
				}
			} else if v == nil {
				if p.auxOpts.NullKeysAllowed {
					continue
				}
				if p.schema.Columns()[fIdx].PrimaryKey || p.schema.Columns()[fIdx].Required {
					return p.newUnparsed(partition, line, fmt.Sprintf("lookupComplex nil %v required", key.ColumnName), idx, msg)
				}
			} else {
				if vv, err := p.ParseVal(v, key.DataType); err != nil {
					if p.auxOpts.NullKeysAllowed {
						continue
					}
					return p.newUnparsed(partition, line, fmt.Sprintf("ParseVal error %v raw(%v): %v", key.ColumnName, v, err.Error()), idx, msg)
				} else {
					changeItem.ColumnValues[fIdx] = vv
				}
			}
		} else {
			rawV, ok := item[key.ColPath()]
			if !ok {
				// try to find value in system columns
				if key.ColPath() == "_lb_ctime" {
					rawV = msg.CreateTime
				} else if key.ColPath() == "_lb_wtime" {
					rawV = msg.WriteTime
				} else if eV, ok := msg.Headers[fmt.Sprintf("_lb_extra_%v", key.ColPath())]; ok {
					rawV = eV
				}
			}
			if v, err := p.ParseVal(rawV, key.DataType); err != nil {
				if (!p.auxOpts.NullKeysAllowed && key.PrimaryKey) || key.Required {
					return p.newUnparsed(partition, line, fmt.Sprintf("ParseVal error %v raw(%v): %v", key.ColumnName, rawV, err.Error()), idx, msg)
				}

				continue
			} else {
				if v == nil && (p.schema.Columns()[fIdx].PrimaryKey || p.schema.Columns()[fIdx].Required) && !p.auxOpts.NullKeysAllowed {
					return p.newUnparsed(partition, line, fmt.Sprintf("ParseVal nil %v raw(%v)", key.ColumnName, rawV), idx, msg)
				}
				item[key.ColumnName] = v
				changeItem.ColumnValues[fIdx] = v
			}
		}
	}

	if p.auxOpts.AddRest {
		rest := make(map[string]interface{})
		for k, v := range item {
			if !p.known[k] {
				rest[k] = v
			}
		}

		changeItem.ColumnValues[p.restIDX()] = rest
	}
	if p.auxOpts.AddTopicColumn {
		changeItem.ColumnValues[p.topicIDX()] = msg.Headers["logtype"]
	}

	tsCol, err := p.extractTimestamp(item, msg.WriteTime)
	if err != nil && !p.auxOpts.NullKeysAllowed {
		return p.newUnparsed(partition, line, fmt.Sprintf("extractTimestamp error: %v", err.Error()), idx, msg)
	}

	if p.auxOpts.AddDedupeKeys {
		changeItem.ColumnValues[TimestampIDX(p.columns)] = tsCol
		changeItem.ColumnValues[PartitionIDX(p.columns)] = changeItem.PartID
		changeItem.ColumnValues[OffsetIDX(p.columns)] = msg.Offset
		changeItem.ColumnValues[ElemIDX(p.columns)] = uint32(idx)
	}
	changeItem.Table = p.TableSplitter(tableName(partition, p.name), item, changeItem.ColumnValues)
	return changeItem
}

func (p *GenericParser) DoBatch(batch parsers.MessageBatch) []abstract.ChangeItem {
	partition := abstract.NewPartition(batch.Topic, batch.Partition)
	parsed := make([]abstract.ChangeItem, 0)
	wCh := make([]chan []abstract.ChangeItem, len(batch.Messages))
	sem := semaphore.NewWeighted(int64(runtime.GOMAXPROCS(0)))
	for i, m := range batch.Messages {
		rChan := make(chan []abstract.ChangeItem, 1)
		wCh[i] = rChan
		_ = sem.Acquire(context.Background(), 1)
		go func(i int, m parsers.Message) {
			defer sem.Release(1)
			changeItems := p.Do(
				m,
				partition,
			)
			if len(changeItems) == 0 {
				p.logger.Warnf("lb_chunk with lb_offset %v has zero parsed change_items", m.Offset)
			}
			wCh[i] <- changeItems
		}(i, m)
	}
	for _, ch := range wCh {
		res := <-ch
		parsed = append(parsed, res...)
	}
	if p.auxOpts.Sniff {
		sniffed := abstract.Sniff(parsed)
		if sniffed != "" {
			p.logger.Info(abstract.Sniff(parsed))
		}
	}
	return parsed
}

func (p *GenericParser) Do(msg parsers.Message, partition abstract.Partition) []abstract.ChangeItem {
	start := time.Now()
	var res []abstract.ChangeItem
	if p.lfParser {
		res = p.doLfParser(msg, partition)
	} else {
		res = p.doGenericParser(msg, partition)
	}
	p.metrics.DecodeTime.RecordDuration(time.Since(start))
	p.logger.Debug(
		"Done generic parse",
		log.Any("elapsed", time.Since(start)),
		log.Any("size", format.Size(binary.Size(msg.Value)).String()),
	)
	return res
}

func (p *GenericParser) doLfParser(msg parsers.Message, partition abstract.Partition) []abstract.ChangeItem {
	res := make([]abstract.ChangeItem, 0)
	idx := 0
	st := time.Now()
	transportMeta := fmt.Sprintf(
		"%v@@%v@@%v@@%v@@%v@@%v@@%v@@%v@@",
		partition.LegacyShittyString(),
		msg.Offset,
		string(msg.Key),
		msg.CreateTime.UnixNano()/int64(time.Millisecond),
		time.Now().Second(),
		p.auxOpts.Topic,
		msg.SeqNo,
		msg.WriteTime.UnixNano()/int64(time.Millisecond),
	)
	parsedYson := lib.Parse(p.lfCfg.ParserName, p.lfCfg.SplitterName, transportMeta, p.auxOpts.MaskSecrets, msg)
	p.logger.Debugf("lib.Parse(%v, %v, %v) %v -> %v in %v", p.lfCfg.ParserName, p.lfCfg.SplitterName, p.auxOpts.MaskSecrets, len(msg.Value), len(parsedYson), time.Since(st))
	st = time.Now()
	reader := yson.NewReaderKindFromBytes([]byte(parsedYson), yson.StreamListFragment)
	for {
		ok, err := reader.NextListItem()
		if err != nil {
			p.logger.Debugf("reader.NextListItem err %v", err)
			break
		}
		if !ok {
			p.logger.Debug("done")
			break
		}
		current, err := reader.NextRawValue()
		if err != nil {
			p.logger.Warnf("reader.NextRawValue err %v", err)
			break
		}
		items, err := newLfResult(current)
		if err != nil {
			p.logger.Warnf("unable to parse result from logfeller parser: %v", err)
			break
		}
		for _, i := range items {
			idx++
			var ci abstract.ChangeItem

			if i.IsUnparsed() {
				ci = p.newUnparsed(partition, i.RawLine, i.Error, idx, msg)
			} else {
				ci = p.makeChangeItem(i.ParsedRecord, idx, i.RawLine, partition, msg)
				if err = abstract.ValidateChangeItem(&ci); err != nil {
					p.logger.Error(err.Error())
				}
			}
			if ci.Table == fmt.Sprintf("%v_unparsed", tableName(partition, p.name)) {
				p.metrics.Unparsed.Inc()
				if p.auxOpts.DropUnparsed {
					continue
				}
			}
			res = append(res, ci)
		}
	}
	p.logger.Debugf("parsed %v -> %v in %v", len(parsedYson), len(res), time.Since(st))
	return res
}

func (p *GenericParser) doGenericParser(msg parsers.Message, partition abstract.Partition) []abstract.ChangeItem {
	res := make([]abstract.ChangeItem, 0)
	idx := 0
	// TODO(@kry127) implement secrets processing in generic parser
	scanner := bufio.NewScanner(bytes.NewReader(msg.Value))
	bufSize := len(msg.Value) + 2
	buf := make([]byte, 0, bufSize)
	scanner.Buffer(buf, bufSize)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}
		idx++

		var item map[string]interface{}
		if err := p.Unmarshal(line, &item); err == nil && len(item) > 0 {
			ci := p.makeChangeItem(item, idx, line, partition, msg)
			if err = abstract.ValidateChangeItem(&ci); err != nil {
				p.logger.Error(err.Error())
			}
			if ci.Table == fmt.Sprintf("%v_unparsed", tableName(partition, p.name)) {
				p.metrics.Unparsed.Inc()
				if p.auxOpts.DropUnparsed {
					continue
				}
			}
			res = append(res, ci)
		} else if err != nil {
			p.metrics.Unparsed.Inc()
			if !p.auxOpts.DropUnparsed {
				res = append(res, p.newUnparsed(partition, line, err.Error(), idx, msg))
			}
		}
	}
	return res
}

func (p *GenericParser) newUnparsed(partition abstract.Partition, line, reason string, idx int, msg parsers.Message) abstract.ChangeItem {
	return NewUnparsed(partition, p.name, line, reason, idx, msg.Offset, msg.WriteTime)
}

func replaceProblemSymbols(in string) string {
	result := in
	result = strings.ReplaceAll(result, "/", "_")
	result = strings.ReplaceAll(result, "@", "_")
	return result
}

func tableName(part abstract.Partition, name string) string {
	if name != "" {
		return replaceProblemSymbols(name)
	}
	return replaceProblemSymbols(part.Topic)
}

func NewUnparsed(partition abstract.Partition, name, line, reason string, idx int, offset uint64, writeTime time.Time) abstract.ChangeItem {
	return abstract.ChangeItem{
		ID:          0,
		LSN:         offset,
		CommitTime:  uint64(writeTime.UnixNano()),
		Counter:     0,
		Kind:        abstract.InsertKind,
		Schema:      "",
		Table:       fmt.Sprintf("%v_unparsed", tableName(partition, name)),
		PartID:      "",
		ColumnNames: UnparsedCols,
		ColumnValues: []interface{}{
			time.Now(),
			partition.String(),
			offset,
			uint32(idx),
			line,
			reason,
		},
		TableSchema: UnparsedSchema,
		OldKeys:     abstract.OldKeysType{KeyNames: nil, KeyTypes: nil, KeyValues: nil},
		TxID:        "",
		Query:       "",
		Size:        abstract.RawEventSize(uint64(len(line))),
	}
}

func wrapIntoEmptyInterface(v *fastjson.Value, useNumbers bool) interface{} {
	if v == nil {
		return nil
	}
	switch v.Type() {
	case fastjson.TypeObject:
		m := make(map[string]interface{})
		v.GetObject().Visit(func(k []byte, v *fastjson.Value) {
			m[string(k)] = wrapIntoEmptyInterface(v, useNumbers)
		})
		return m
	case fastjson.TypeArray:
		a := make([]interface{}, 0)
		for _, v := range v.GetArray() {
			a = append(a, wrapIntoEmptyInterface(v, useNumbers))
		}
		return a
	case fastjson.TypeString:
		return string(v.GetStringBytes())
	case fastjson.TypeTrue, fastjson.TypeFalse:
		return v.GetBool()
	case fastjson.TypeNumber:
		if useNumbers {
			// v.String do wierd magic trick that prevent escape string to heap
			return json.Number(v.String())
		}
		return v.GetFloat64()
	default:
		return nil
	}
}

func tryToUnescapeJSON(input string) string {
	var out string
	if err := json.Unmarshal([]byte(input), &out); err == nil {
		return out
	}
	return input
}

func tryToUnescapeTSKV(input string) string {
	var b strings.Builder
	const escapeSymbol = '\\'
	unescapeMap := map[byte]byte{
		'\\': '\\',
		'n':  '\n',
		'r':  '\r',
		't':  '\t',
		'=':  '=',
	}
	for i := 0; i < len(input); i++ {
		if input[i] != escapeSymbol {
			b.WriteByte(input[i])
			continue
		}
		// if single escape symbol in end of string
		if i == len(input)-1 {
			return input
		}
		out, ok := unescapeMap[input[i+1]]
		if !ok {
			return input
		}
		b.WriteByte(out)
		i++
	}
	return b.String()
}

func (p *GenericParser) Unmarshal(line string, item *map[string]interface{}) error {
	switch p.genericCfg.Format {
	case "json":
		{
			parser := p.jsonParserPool.Get()
			defer p.jsonParserPool.Put(parser)
			v, err := parser.Parse(line)
			if err != nil {
				return xerrors.Errorf("unable to parser line %s, err: %w", line, err)
			}
			res := make(map[string]interface{})
			v.GetObject().Visit(func(k []byte, v *fastjson.Value) {
				if v == nil {
					return
				}
				if v.Type() == fastjson.TypeNull {
					res[string(k)] = nil
					return
				}

				if v.Type() == fastjson.TypeString {
					val := string(v.GetStringBytes())
					if p.auxOpts.UnescapeStringValues {
						val = tryToUnescapeJSON(val)
					}
					res[string(k)] = val
					return
				}

				switch schema.Type(p.colTypeMap[string(k)]) {
				case schema.TypeString, schema.TypeBytes:
					res[string(k)] = v.String()
				case schema.TypeFloat64:
					res[string(k)] = v.GetFloat64()
				case schema.TypeBoolean:
					res[string(k)] = v.GetBool()
				case schema.TypeInt8:
					res[string(k)] = int8(v.GetInt())
				case schema.TypeInt16:
					res[string(k)] = int16(v.GetInt())
				case schema.TypeInt32:
					res[string(k)] = int32(v.GetInt())
				case schema.TypeInt64:
					res[string(k)] = v.GetInt64()
				case schema.TypeUint8:
					res[string(k)] = uint8(v.GetUint())
				case schema.TypeUint16:
					res[string(k)] = uint16(v.GetUint())
				case schema.TypeUint32:
					res[string(k)] = uint32(v.GetUint())
				case schema.TypeUint64:
					res[string(k)] = v.GetUint64()
				default:
					r := wrapIntoEmptyInterface(v.Get(), p.auxOpts.UseNumbersInAny)
					res[string(k)] = r
				}
			})
			*item = res
			return nil
		}
	case "tskv":
		v := make(map[string]interface{})
		for _, f := range strings.Split(line, "\t") {
			parts := strings.SplitN(f, "=", 2)
			if len(parts) != 2 {
				continue
			}
			val := parts[1]
			if p.auxOpts.UnescapeStringValues {
				val = tryToUnescapeTSKV(val)
			}
			v[parts[0]] = val
		}

		*item = v
	case "redir":
		v := make(map[string]interface{})
		for _, f := range strings.Split(line, "@@") {
			parts := strings.SplitN(f, "=", 2)
			if len(parts) != 2 {
				continue
			}
			v[parts[0]] = parts[1]
		}

		*item = v
	default:
		d := yson.NewDecoderFromBytes([]byte(line))
		err := d.Decode(item)
		if err != nil {
			return xerrors.Errorf("unable to decode string %s, err: %w", line, err)
		}
		return nil
	}

	return nil
}

func (p *GenericParser) topicIDX() int {
	return len(p.columns) - 6
}

func (p *GenericParser) restIDX() int {
	return len(p.columns) - 5
}

func TimestampIDX(columns []string) int {
	return len(columns) - 4
}

func PartitionIDX(columns []string) int {
	return len(columns) - 3
}

func OffsetIDX(columns []string) int {
	return len(columns) - 2
}

func ElemIDX(columns []string) int {
	return len(columns) - 1
}

// can return in moment: valid value & error
func (p *GenericParser) extractTimestamp(row map[string]interface{}, defaultTime time.Time) (time.Time, error) {
	if p.auxOpts.TimeField != nil && row[p.auxOpts.TimeField.Col] != nil {
		resultTime, done, err := p.extractTimeValue(row[p.auxOpts.TimeField.Col], defaultTime)
		if done {
			if resultTime == nil {
				if err == nil {
					return defaultTime, nil
				} else {
					return defaultTime, xerrors.Errorf("unable to extract time value (return default) from val %v, err: %w", row[p.auxOpts.TimeField.Col], err)
				}
			}
			if err == nil {
				return *resultTime, err
			} else {
				return *resultTime, xerrors.Errorf("unable to extract time value (return extracted) from val %v, err: %w", row[p.auxOpts.TimeField.Col], err)
			}

		}
	}

	return defaultTime, nil
}

func (p *GenericParser) extractTimeValue(col interface{}, defaultTime time.Time) (*time.Time, bool, error) {
	if col == nil {
		return nil, true, nil
	}
	switch v := col.(type) {
	case string:
		if p.auxOpts.TimeField != nil && p.auxOpts.TimeField.Format != "" {
			if t, err := time.Parse(p.auxOpts.TimeField.Format, v); err == nil {
				return &t, true, nil
			} else {
				return nil, false, xerrors.Errorf(
					"unable to parse date %v in format %v: %w",
					v,
					p.auxOpts.TimeField.Format, err,
				)
			}
		}

		if p.auxOpts.InferTimeZone {
			if t, err := dateparse.ParseAny(v); err == nil {
				return &t, true, nil
			}
		}

		if t, err := dateparse.ParseLocal(v); err == nil {
			return &t, true, nil
		} else {
			if t, err := time.Parse("2006.01.02 15:04:05.999999", v); err == nil {
				return &t, true, nil
			}
			return &t, true, xerrors.Errorf("unable to parse: %v: err: %w", v, err)
		}
	case time.Time:
		return &v, true, nil
	case *time.Time:
		return v, true, nil
	case json.Number:
		extracted, err := v.Int64()
		if err != nil {
			return &defaultTime, false, err
		}
		t := time.Unix(extracted, 0)
		return &t, true, nil
	case int64:
		t := time.Unix(v, 0)
		return &t, true, nil
	case uint64:
		t := time.Unix(int64(v), 0)
		return &t, true, nil
	case int32:
		t := time.Unix(int64(v), 0)
		return &t, true, nil
	case uint32:
		t := time.Unix(int64(v), 0)
		return &t, true, nil
	case float64:
		t := time.Unix(int64(math.Abs(v)), 0)
		return &t, true, nil
	default:
		return &defaultTime, true, xerrors.Errorf("unable extract timestamp: %v (%T)", v, v)
	}
}

func (p *GenericParser) ParseVal(v interface{}, typ string) (interface{}, error) {
	if strings.ToLower(typ) == "datetime" {
		t, _, err := p.extractTimeValue(v, time.Now())
		if err != nil {
			return nil, xerrors.Errorf("unable to extract time: %w", err)
		}
		if t == nil {
			return nil, nil
		}
		return *t, nil
	}

	if n, ok := v.(float64); ok {
		switch schema.Type(typ) {
		case schema.TypeFloat64:
			return n, nil
		case schema.TypeInt8:
			return int8(n), nil
		case schema.TypeInt16:
			return int16(n), nil
		case schema.TypeInt32:
			return int32(n), nil
		case schema.TypeInt64:
			return int64(n), nil
		case schema.TypeUint8:
			return uint8(n), nil
		case schema.TypeUint16:
			return uint16(n), nil
		case schema.TypeUint32:
			return uint32(n), nil
		case schema.TypeUint64:
			return uint64(n), nil
		case schema.TypeString, schema.TypeBytes:
			return fmt.Sprintf("%v", v), nil
		default:
			return n, nil
		}
	}

	if vv, ok := v.(json.Number); ok {
		switch schema.Type(typ) {
		case schema.TypeFloat64:
			result, err := fastfloat.Parse(vv.String())
			if err != nil {
				return nil, xerrors.Errorf("unable to parse float64 %v:, err: %w", vv, err)
			}
			return result, nil
		case schema.TypeInt8:
			n, err := vv.Int64()
			if err != nil {
				return nil, xerrors.Errorf("unable to parse json.Number to %v: %w", typ, err)
			}
			return int8(n), nil
		case schema.TypeInt16:
			n, err := vv.Int64()
			if err != nil {
				return nil, xerrors.Errorf("unable to parse json.Number to %v: %w", typ, err)
			}
			return int16(n), nil
		case schema.TypeInt32:
			n, err := vv.Int64()
			if err != nil {
				return nil, xerrors.Errorf("unable to parse json.Number to %v: %w", typ, err)
			}
			return int32(n), nil
		case schema.TypeInt64:
			n, err := vv.Int64()
			if err != nil {
				return nil, xerrors.Errorf("unable to parse json.Number to %v: %w", typ, err)
			}
			return n, nil
		case schema.TypeUint8:
			n, err := vv.Int64()
			if err != nil {
				return nil, xerrors.Errorf("unable to parse json.Number to %v: %w", typ, err)
			}
			return uint8(n), nil
		case schema.TypeUint16:
			n, err := vv.Int64()
			if err != nil {
				return nil, xerrors.Errorf("unable to parse json.Number to %v: %w", typ, err)
			}
			return uint16(n), nil
		case schema.TypeUint32:
			n, err := vv.Int64()
			if err != nil {
				return nil, xerrors.Errorf("unable to parse json.Number to %v: %w", typ, err)
			}
			return uint32(n), nil
		case schema.TypeUint64:
			result, err := strconv.ParseUint(vv.String(), 10, 64)
			if err != nil {
				return nil, xerrors.Errorf("unable to parse uint %v:, err: %w", vv, err)
			}
			return result, nil
		case schema.TypeString, schema.TypeBytes:
			return vv.String(), nil
		case schema.TypeAny:
			return vv, nil
		default:
			result, err := fastfloat.Parse(vv.String())
			if err != nil {
				return nil, xerrors.Errorf("unable to parse float64 %v:, err: %w", vv, err)
			}
			return result, nil
		}
	}

	if vv, ok := v.(string); ok {
		switch schema.Type(typ) {
		case schema.TypeFloat64:
			result, err := strconv.ParseFloat(vv, 64)
			if err != nil {
				return nil, xerrors.Errorf("unable to parse float %v:, err: %w", vv, err)
			}
			return result, nil
		case schema.TypeBoolean:
			result, err := strconv.ParseBool(vv)
			if err != nil {
				return nil, xerrors.Errorf("unable to parse bool %v:, err: %w", vv, err)
			}
			return result, nil
		case schema.TypeInt8:
			i, err := strconv.ParseInt(vv, 0, 8)
			if err != nil {
				return nil, xerrors.Errorf("unable to parse string to %v: %w", typ, err)
			}
			return int8(i), nil
		case schema.TypeInt16:
			i, err := strconv.ParseInt(vv, 0, 16)
			if err != nil {
				return nil, xerrors.Errorf("unable to parse string to %v: %w", typ, err)
			}
			return int16(i), nil
		case schema.TypeInt32:
			i, err := strconv.ParseInt(vv, 0, 32)
			if err != nil {
				return nil, xerrors.Errorf("unable to parse string to %v: %w", typ, err)
			}
			return int32(i), nil
		case schema.TypeInt64:
			i, err := strconv.ParseInt(vv, 0, 64)
			if err != nil {
				return nil, xerrors.Errorf("unable to parse string to %v: %w", typ, err)
			}
			return i, nil
		case schema.TypeUint8:
			i, err := strconv.ParseUint(vv, 0, 8)
			if err != nil {
				return nil, xerrors.Errorf("unable to parse string to %v: %w", typ, err)
			}
			return uint8(i), nil
		case schema.TypeUint16:
			i, err := strconv.ParseUint(vv, 0, 16)
			if err != nil {
				return nil, xerrors.Errorf("unable to parse string to %v: %w", typ, err)
			}
			return uint16(i), nil
		case schema.TypeUint32:
			i, err := strconv.ParseUint(vv, 0, 32)
			if err != nil {
				return nil, xerrors.Errorf("unable to parse string to %v: %w", typ, err)
			}
			return uint32(i), nil
		case schema.TypeUint64:
			i, err := strconv.ParseUint(vv, 0, 64)
			if err != nil {
				return nil, xerrors.Errorf("unable to parse string to %v: %w", typ, err)
			}
			return i, nil
		case schema.TypeBytes:
			if p.auxOpts.UnpackBytesBase64 {
				// will try to base64 decode values out of parser
				decodedV, err := base64.StdEncoding.DecodeString(vv)
				if err != nil {
					return nil, xerrors.Errorf("unable to decode base64 bytes: %w", err)
				}
				return decodedV, nil
			}
			return v, nil
		case schema.TypeAny:
			var res map[string]interface{}
			// TODO: (tserakhau) fixme
			vv = strings.ReplaceAll(vv, "\\\\", "\\") // double escaping taxi bug
			if err := json.Unmarshal([]byte(vv), &res); err == nil {
				return res, nil
			}
			return vv, nil
		default:
			return v, nil
		}
	}

	switch schema.Type(typ) {
	case schema.TypeTimestamp:
		switch t := v.(type) {
		case int64:
			return schema.Timestamp(t).Time(), nil
		case uint64:
			return schema.Timestamp(t).Time(), nil
		}
	case schema.TypeInterval:
		switch t := v.(type) {
		case schema.Interval:
			duration := time.Duration(int64(t) * 1000)
			return duration, nil
		case int64:
			duration := time.Duration(t * 1000)
			return duration, nil
		}
	}
	return v, nil
}

func splitStepTableName(tableName, val string) string {
	return tableName + val + "/"
}

func TableSplitter(sourceName string, columns []string, item map[string]interface{}, columnValues []interface{}, auxFieldsToIndex map[string]int) string {
	tableName := ""
	for _, col := range columns {
		val, ok := item[col]
		if !ok {
			if index, ok := auxFieldsToIndex[col]; ok {
				val = columnValues[index]
			}
		}
		if val, err := castx.ToStringE(val); err == nil {
			tableName = splitStepTableName(tableName, val)
		}
	}
	if len(tableName) > 0 {
		tableName = tableName[:len(tableName)-1]
		return tableName
	}
	return sourceName
}

func (p *GenericParser) TableSplitter(sourceName string, item map[string]interface{}, columnValues []interface{}) string {
	if p.auxOpts.TableSplitter != nil {
		return TableSplitter(sourceName, p.auxOpts.TableSplitter.Columns, item, columnValues, p.auxFieldsToIndex)
	}
	return sourceName
}

func (p *GenericParser) Name() string {
	return p.name
}

func (p *GenericParser) Opts() AuxParserOpts {
	return *p.auxOpts
}

func (p *GenericParser) SetTopic(topicName string) {
	p.name = strings.ReplaceAll(topicName, "/", "_")
	p.auxOpts.Topic = topicName
}

func (p *GenericParser) ResultSchema() *abstract.TableSchema {
	return p.schema
}

func checkParserCorrectness(opts AuxParserOpts, sch []abstract.ColSchema, lgr log.Logger) {
	if opts.InferTimeZone || (opts.TimeField != nil && opts.TimeField.Format != "") {
		return
	}

	hasDatetime := false
	for _, col := range sch {
		if col.DataType == schema.TypeDatetime.String() {
			hasDatetime = true
			break
		}
	}

	if hasDatetime {
		lgr.Warn("the datetime fields will be parsed in local timezone, because infer timezone and time field are not configured")
	}
}

func NewGenericParser(cfg ParserConfig, fields []abstract.ColSchema, logger log.Logger, registry *stats.SourceStats) *GenericParser {
	var opts *AuxParserOpts
	var lfCfg *LogfellerParserConfig
	var genericCfg *GenericParserConfig
	var isLf bool
	switch parserOpts := cfg.(type) {
	case *LogfellerParserConfig:
		logger.Infof("Run Logfeller parser %v", parserOpts)
		opts = &parserOpts.AuxOpts
		isLf = true
		lfCfg = parserOpts
	case *GenericParserConfig:
		logger.Infof("Run Generic parser")
		opts = &parserOpts.AuxOpts
		genericCfg = parserOpts
	default:
		logger.Errorf("Cannot create generic parser: expected LogfellerParserConfig or GenericParserConfig, but got %v", fmt.Sprintf("%T", cfg))
		return nil
	}

	finalSchema := addAuxFields(fields, *opts)
	auxFieldsToIndex := auxFieldsToIndex(fields, finalSchema)

	known := make(map[string]bool)
	for _, col := range fields {
		known[col.ColumnName] = true
		if !opts.IgnoreColumnPaths {
			known[col.ColPath()] = true
		}
	}

	colNames := make([]string, len(finalSchema))
	for i, k := range finalSchema {
		colNames[i] = k.ColumnName
	}
	colType := make(map[string]string, len(finalSchema))
	for _, col := range finalSchema {
		if opts.IgnoreColumnPaths {
			colType[col.ColumnName] = col.DataType
		} else {
			colType[col.ColPath()] = col.DataType
		}
	}
	logger.Info("Final schema", log.Any("columns", colNames), log.Any("s", finalSchema))
	checkParserCorrectness(*opts, finalSchema, logger)
	return &GenericParser{
		rawFields:        fields,
		known:            known,
		schema:           abstract.NewTableSchema(finalSchema),
		auxFieldsToIndex: auxFieldsToIndex,
		columns:          colNames,
		colTypeMap:       colType,
		logger:           logger,
		metrics:          registry,
		lfParser:         isLf,
		lfCfg:            lfCfg,
		genericCfg:       genericCfg,
		auxOpts:          opts,
		name:             strings.ReplaceAll(opts.Topic, "/", "_"),
		jsonParserPool:   fastjson.ParserPool{},
	}
}
