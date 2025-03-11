package generic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/base"
	"github.com/transferria/transferria/pkg/base/adapter"
	"github.com/transferria/transferria/pkg/base/events"
	"github.com/transferria/transferria/pkg/parsers"
	"github.com/transferria/transferria/pkg/parsers/registry/logfeller/lib"
	"github.com/valyala/fastjson"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yson"
	"golang.org/x/sync/semaphore"
)

type ysonEventBatch struct {
	yson          string
	iter          int
	reader        *yson.Reader
	events        []events.InsertEvent
	parser        *GenericParser
	partition     abstract.Partition
	parsedTable   base.Table
	unparsedTable base.Table
	parseErr      error
	msg           parsers.Message
}

func (p *GenericParser) baseTable(partition abstract.Partition) base.Table {
	return adapter.NewTableFromLegacy(p.schema, abstract.TableID{
		Name:      partition.Topic,
		Namespace: "",
	})
}

func (p *GenericParser) baseUnparsedTable(partition abstract.Partition) base.Table {
	return adapter.NewTableFromLegacy(UnparsedSchema, abstract.TableID{
		Namespace: fmt.Sprintf("%v_unparsed", partition.Topic),
		Name:      "",
	})
}

func (p *GenericParser) ParseBatch(batch parsers.MessageBatch) base.EventBatch {
	partition := abstract.NewPartition(batch.Topic, batch.Partition)
	wCh := make([]chan base.EventBatch, len(batch.Messages))
	sem := semaphore.NewWeighted(int64(runtime.GOMAXPROCS(0)))
	for i, m := range batch.Messages {
		rChan := make(chan base.EventBatch, 1)
		wCh[i] = rChan
		_ = sem.Acquire(context.Background(), 1)
		go func(i int, m parsers.Message) {
			defer sem.Release(1)
			batch := p.Parse(m, partition)
			wCh[i] <- batch
		}(i, m)
	}
	var eventBatches []base.EventBatch
	for _, ch := range wCh {
		eventBatches = append(eventBatches, <-ch)
	}
	return base.NewBatchFromBatches(eventBatches)
}

func (p *GenericParser) Parse(msg parsers.Message, partition abstract.Partition) base.EventBatch {
	if p.lfParser {
		return p.logfellerParse(msg, partition)
	}

	return p.genericParse(msg, partition)
}

func (p *GenericParser) genericParse(msg parsers.Message, partition abstract.Partition) base.EventBatch {
	table := adapter.NewTableFromLegacy(p.schema, abstract.TableID{
		Name:      partition.Topic,
		Namespace: "",
	})
	unparsedTable := adapter.NewTableFromLegacy(p.schema, abstract.TableID{
		Name:      partition.Topic,
		Namespace: "",
	})
	partStr := partition.String()
	var items []base.Event
	scanner := bufio.NewScanner(bytes.NewReader(msg.Value))
	bufSize := len(msg.Value) + 2
	buf := make([]byte, 0, bufSize)
	scanner.Buffer(buf, bufSize)
	idx := 0
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}
		idx++

		bldr, rest, err := p.parseLine(table, line)
		if err != nil {
			ev, bErr := events.NewDefaultInsertBuilder(unparsedTable).
				Column("_timestamp", msg.WriteTime).
				Column("_partition", partStr).
				Column("_offset", msg.Offset).
				Column("_idx", idx).
				Column("unparsed_row", line).
				Column("reason", err.Error()).
				Build()
			if bErr != nil {
				p.logger.Warn("unable to build unparsed event", log.Error(err))
			}
			if ev != nil && !p.auxOpts.DropUnparsed {
				items = append(items, ev)
			}
			continue
		}
		if p.auxOpts.AddRest {
			bldr.Column("_rest", rest)
		}
		bldr.Column("_timestamp", msg.WriteTime)
		bldr.Column("_idx", idx)
		bldr.Column("_offset", msg.Offset)
		bldr.Column("_partition", partStr)
		ev, err := bldr.Build()
		if err != nil {
			ev, bErr := events.NewDefaultInsertBuilder(unparsedTable).
				Column("_timestamp", msg.WriteTime).
				Column("_partition", partStr).
				Column("_offset", msg.Offset).
				Column("_idx", idx).
				Column("unparsed_row", line).
				Column("reason", err.Error()).
				Build()
			if bErr != nil {
				p.logger.Warn("unable to build unparsed event", log.Error(err))
			}
			if ev != nil && !p.auxOpts.DropUnparsed {
				items = append(items, ev)
			}
		}
		items = append(items, ev)
	}
	return base.NewEventBatch(items)
}

func (p *GenericParser) parseLine(table base.Table, line string) (events.InsertBuilder, map[string]interface{}, error) {
	switch p.genericCfg.Format {
	case "json":
		builder, rest, err := p.parseJSONLine(table, line)
		if err != nil {
			return nil, nil, xerrors.Errorf("unable to parser json line: %w", err)
		}
		return builder, rest, nil
	case "tskv":
		builder, rest, err := p.parseTSKVLine(table, line)
		if err != nil {
			return nil, nil, xerrors.Errorf("unable to parser tskv line: %w", err)
		}
		return builder, rest, nil
	default:
		return nil, nil, xerrors.Errorf("format: %v not supported", p.genericCfg.Format)
	}
}

func (p *GenericParser) parseJSONLine(table base.Table, line string) (events.InsertBuilder, map[string]interface{}, error) {
	blrd := events.NewDefaultInsertBuilder(table)
	parser := p.jsonParserPool.Get()
	defer p.jsonParserPool.Put(parser)
	v, err := parser.Parse(line)
	if err != nil {
		return nil, nil, xerrors.Errorf("unable parse line: %w", err)
	}
	rest := make(map[string]interface{})
	var nested []abstract.ColSchema
	cols, err := table.ToOldTable()
	if err != nil {
		return nil, nil, xerrors.Errorf("unable convert table into old format: %w", err)
	}
	for _, col := range cols.Columns() {
		if col.Path != "" && col.Path != col.ColumnName {
			nested = append(nested, col)
			continue
		}
	}

	v.GetObject().Visit(func(k []byte, v *fastjson.Value) {
		if v == nil {
			return
		}
		key := string(k)
		if !p.known[key] && p.auxOpts.AddRest {
			rest[key] = wrapIntoEmptyInterface(v.Get(), p.auxOpts.UseNumbersInAny)
			return
		}
		if v.Type() == fastjson.TypeNull {
			blrd.Column(key, nil)
			return
		}

		if !p.known[key] {
			// add to rest
			rest[key] = wrapIntoEmptyInterface(v.Get(), p.auxOpts.UseNumbersInAny)
		}
		switch schema.Type(strings.ToLower(p.colTypeMap[key])) {
		case schema.TypeDatetime, schema.TypeDate, schema.TypeTimestamp:
			t, ok, err := p.extractTimeValue(string(v.GetStringBytes()), time.Now())
			if !ok || err != nil {
				blrd.Column(key, nil)
			}
			blrd.Column(key, t)
		case schema.TypeString, schema.TypeBytes:
			blrd.Column(key, string(v.GetStringBytes()))
		case schema.TypeFloat64:
			blrd.Column(key, v.GetFloat64())
		case schema.TypeBoolean:
			blrd.Column(key, v.GetBool())
		case schema.TypeInt8:
			blrd.Column(key, int8(v.GetInt()))
		case schema.TypeInt16:
			blrd.Column(key, int16(v.GetInt()))
		case schema.TypeInt32:
			blrd.Column(key, int32(v.GetInt()))
		case schema.TypeInt64:
			blrd.Column(key, int64(v.GetInt()))
		case schema.TypeUint8:
			blrd.Column(key, uint8(v.GetUint()))
		case schema.TypeUint16:
			blrd.Column(key, uint16(v.GetUint()))
		case schema.TypeUint32:
			blrd.Column(key, uint32(v.GetUint()))
		case schema.TypeUint64:
			blrd.Column(key, v.GetUint64())
		default:
			r := wrapIntoEmptyInterface(v.Get(), p.auxOpts.UseNumbersInAny)
			blrd.Column(key, r)
		}
	})
	if err := p.fillNested(nested, rest, blrd); err != nil {
		return nil, nil, xerrors.Errorf("unable fill nested: %w", err)
	}
	return blrd, rest, nil
}

func (p *GenericParser) parseTSKVLine(table base.Table, line string) (events.InsertBuilder, map[string]interface{}, error) {
	blrd := events.NewDefaultInsertBuilder(table)

	v := make(map[string]interface{})
	var nested []abstract.ColSchema
	cols, err := table.ToOldTable()
	if err != nil {
		return nil, nil, xerrors.Errorf("unable convert table to old format: %w", err)
	}
	for _, col := range cols.Columns() {
		if col.Path != "" && col.Path != col.ColumnName {
			nested = append(nested, col)
			continue
		}
	}

	for _, f := range strings.Split(line, "\t") {
		parts := strings.SplitN(f, "=", 2)
		if len(parts) != 2 {
			continue
		}

		if !p.known[parts[0]] {
			v[parts[0]] = parts[1]
			continue
		}
		col := table.ColumnByName(parts[0])
		old, _ := col.ToOldColumn()
		vv, err := p.ParseVal(parts[1], old.DataType)
		if err != nil {
			if !col.Nullable() {
				return nil, nil, xerrors.Errorf("column: %v required, but parse failed with: %w", col.FullName(), err)
			}
		}
		blrd.Column(parts[0], vv)
	}
	if err := p.fillNested(nested, v, blrd); err != nil {
		return nil, nil, xerrors.Errorf("unable fill nested fields: %w", err)
	}
	return blrd, v, nil
}

func (p *GenericParser) fillNested(nested []abstract.ColSchema, rest map[string]interface{}, blrd events.InsertBuilder) error {
	for _, col := range nested {
		if v, err := lookupComplex(rest, col.Path); err != nil {
			if p.auxOpts.NullKeysAllowed {
				continue
			}
			if col.PrimaryKey || col.Required {
				return xerrors.Errorf("lookupComplex error %v: %w", col.ColumnName, err)
			}
		} else if v == nil {
			if p.auxOpts.NullKeysAllowed {
				continue
			}
			if col.PrimaryKey || col.Required {
				return xerrors.Errorf("lookupComplex nil %v required", col.ColumnName)
			}
		} else {
			if vv, err := p.ParseVal(v, col.DataType); err != nil {
				if p.auxOpts.NullKeysAllowed {
					continue
				}
				return xerrors.Errorf("ParseVal error %v raw(%v): %v", col.ColumnName, v, err)
			} else {
				blrd.Column(col.ColumnName, vv)
			}
		}
	}
	return nil
}

func (l *ysonEventBatch) Next() bool {
	if l.iter == -1 {
		partStr := l.partition.String()
		reader := yson.NewReaderKindFromBytes([]byte(l.yson), yson.StreamListFragment)
		for {
			ok, err := reader.NextListItem()
			if err != nil {
				l.parseErr = xerrors.Errorf("unable to next list item: %w", err)
				return true
			}
			if !ok {
				break
			}
			current, err := reader.NextRawValue()
			if err != nil {
				l.parseErr = xerrors.Errorf("unable to next raw value: %w", err)
				return true
			}
			items, err := newLfResult(current)
			if err != nil {
				l.parseErr = xerrors.Errorf("unable to next extract items: %w", err)
				return true
			}
			idx := uint32(0)
			for _, item := range items {
				idx++
				if item.IsUnparsed() {
					ev, err := events.NewDefaultInsertBuilder(l.unparsedTable).
						Column("_timestamp", l.msg.WriteTime).
						Column("_partition", partStr).
						Column("_offset", l.msg.Offset).
						Column("_idx", idx).
						Column("unparsed_row", item.RawLine).
						Column("reason", item.Error).
						Build()
					if err != nil {
						l.parseErr = xerrors.Errorf("unable to construct unparsed event: %w", err)
						return true
					}
					l.events = append(l.events, ev)
				} else {
					if l.parser.auxOpts.AddSystemColumns {
						item.ParsedRecord["_lb_ctime"] = l.msg.CreateTime
						item.ParsedRecord["_lb_wtime"] = l.msg.WriteTime
						for k, v := range l.msg.Headers {
							item.ParsedRecord[fmt.Sprintf("_lb_extra_%v", k)] = v
						}
					}
					ev, err := events.NewDefaultInsertBuilder(l.parsedTable).
						FromMap(item.ParsedRecord).
						Column("_timestamp", l.msg.WriteTime).
						Column("_idx", idx).
						Column("_offset", l.msg.Offset).
						Column("_partition", partStr).
						Build()
					if err != nil {
						l.parseErr = xerrors.Errorf("unable to construct table event: %w", err)
						return true
					}
					l.events = append(l.events, ev)
				}
			}
		}
	}
	if len(l.events)-1 > l.iter {
		l.iter++
		return true
	}
	return false
}

func (l *ysonEventBatch) Event() (base.Event, error) {
	if l.parseErr != nil {
		return nil, l.parseErr
	}
	return l.events[l.iter], nil
}

func (l *ysonEventBatch) Count() int {
	return len(l.events)
}

func (l *ysonEventBatch) Size() int {
	return binary.Size(l.events)
}

func (l *ysonEventBatch) MarshalYSON(w *yson.Writer) error {
	w.BeginList()
	w.RawNode([]byte(l.yson))
	w.EndList()
	return w.Finish()
}

func (p *GenericParser) logfellerParse(msg parsers.Message, partition abstract.Partition) base.EventBatch {
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
	return &ysonEventBatch{
		yson:          parsedYson,
		iter:          -1,
		parser:        p,
		partition:     partition,
		parsedTable:   p.baseTable(partition),
		unparsedTable: p.baseUnparsedTable(partition),
		parseErr:      nil,
		reader:        nil,
		events:        nil,
		msg:           msg,
	}
}
