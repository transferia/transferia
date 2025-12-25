package jsonparser

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cast"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/generic"
	"github.com/transferia/transferia/pkg/parsers/registry/blank"
	jsonparser "github.com/transferia/transferia/pkg/parsers/registry/json"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/transformer"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

const TransformerType = abstract.TransformerType("jsonparser")

func init() {
	transformer.Register[Config](
		TransformerType,
		func(cfg Config, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
			return New(cfg, lgr)
		},
	)
}

type Config struct {
	Parser *jsonparser.ParserConfigJSONCommon
	Topic  string
}

type parser struct {
	topic  string
	parser *generic.GenericParser
}

func changeItemAsMessage(ci *abstract.ChangeItem) (msg parsers.Message, part abstract.Partition, err error) {
	if ci.TableSchema != blank.BlankSchema {
		return parsers.Message{}, abstract.Partition{}, xerrors.Errorf("unexpected schema: %v", ci.TableSchema.Columns())
	}
	var errs util.Errors
	xtras, err := blank.ExtractValue[map[string]string](ci, blank.ExtrasColumn)
	errs = util.AppendErr(errs, err)
	partition, err := blank.ExtractValue[string](ci, blank.PartitionColum)
	errs = util.AppendErr(errs, err)
	seqNo, err := blank.ExtractValue[uint64](ci, blank.SeqNoColumn)
	errs = util.AppendErr(errs, err)
	cTime, err := blank.ExtractValue[time.Time](ci, blank.CreateTimeColumn)
	errs = util.AppendErr(errs, err)
	wTime, err := blank.ExtractValue[time.Time](ci, blank.WriteTimeColumn)
	errs = util.AppendErr(errs, err)
	rawData, err := blank.ExtractValue[[]byte](ci, blank.RawMessageColumn)
	errs = util.AppendErr(errs, err)
	sourceID, err := blank.ExtractValue[string](ci, blank.SourceIDColumn)
	errs = util.AppendErr(errs, err)
	if len(errs) > 0 {
		return msg, part, xerrors.Errorf("format errors: %w", errs)
	}
	if err := json.Unmarshal([]byte(partition), &part); err != nil {
		return msg, part, xerrors.Errorf("unable to parse partition: %w", err)
	}

	return parsers.Message{
		Offset:     ci.LSN,
		SeqNo:      seqNo,
		Key:        []byte(sourceID),
		CreateTime: cTime,
		WriteTime:  wTime,
		Value:      rawData,
		Headers:    xtras,
	}, part, nil
}

func (r parser) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	if len(input) == 0 {
		return abstract.TransformerResult{
			Transformed: nil,
			Errors:      nil,
		}
	}
	var parsed []abstract.ChangeItem
	var errs []abstract.TransformerError
	batches := map[abstract.Partition][]parsers.Message{}
	for _, row := range input {
		msg, part, err := changeItemAsMessage(&row)
		if err != nil {
			errs = append(errs, abstract.TransformerError{
				Input: row,
				Error: err,
			})
			continue
		}
		batches[part] = append(batches[part], msg)
	}

	for part, msgs := range batches {
		parsed = append(parsed, r.parser.DoBatch(parsers.MessageBatch{
			Topic:     fmt.Sprintf("rt3.na--%s", part.Topic), // lol
			Partition: part.Partition,
			Messages:  msgs,
		})...)
	}
	return abstract.TransformerResult{
		Transformed: parsed,
		Errors:      errs,
	}
}

func (r parser) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	if schema != blank.BlankSchema {
		return false
	}
	var part abstract.Partition
	_ = json.Unmarshal([]byte(table.Name), &part)
	return r.topic == part.Topic
}

func (r parser) ResultSchema(_ *abstract.TableSchema) (*abstract.TableSchema, error) {
	return r.parser.ResultSchema(), nil
}

func (r parser) Description() string {
	return fmt.Sprintf(`JSON-parser transformer for topic: %s:
%v
`, r.topic, r.columnsDescription())
}

func (r parser) Type() abstract.TransformerType {
	return TransformerType
}

func (r parser) columnsDescription() string {
	buf := &bytes.Buffer{}

	table := tablewriter.NewWriter(buf)
	table.SetHeaderLine(true)
	table.SetRowLine(true)
	table.SetHeader([]string{"Column", "Type", "Key", "Path"})
	for _, col := range r.parser.ResultSchema().Columns() {
		table.Append([]string{
			col.ColumnName,
			col.DataType,
			cast.ToString(col.PrimaryKey),
			col.Path,
		})
	}
	table.Render()
	return buf.String()
}

func New(cfg Config, lgr log.Logger) (abstract.Transformer, error) {
	p, err := jsonparser.NewParserJSON(cfg.Parser, false, lgr, stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())))
	if err != nil {
		return nil, xerrors.Errorf("unable to construct json parser: %w", err)
	}
	wrappedParser, ok := p.(*parsers.ResourceableParser)
	if !ok {
		return nil, xerrors.Errorf("unknown parser: %T, must be wrapper", p)
	}
	genParser, ok := wrappedParser.Unwrap().(*generic.GenericParser)
	if !ok {
		return nil, xerrors.Errorf("unknown parser: %T, must be generic parser", p)
	}
	return &parser{
		topic:  cfg.Topic,
		parser: genParser,
	}, nil
}
