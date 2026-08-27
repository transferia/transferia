package jsonparser

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cast"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	generic_parser "github.com/transferia/transferia/pkg/parsers/generic"
	parser_blank "github.com/transferia/transferia/pkg/parsers/registry/blank"
	parser_json "github.com/transferia/transferia/pkg/parsers/registry/json"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/transformer"
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
	Parser *parser_json.ParserConfigJSONCommon
	Topic  string
}

type parser struct {
	topic  string
	parser *generic_parser.GenericParser
}

func changeItemAsMessage(ci *abstract.ChangeItem) (msg parsers.Message, part abstract.Partition, err error) {
	if ci.TableSchema != parser_blank.BlankSchema {
		return parsers.Message{}, abstract.NewEmptyPartition(), xerrors.Errorf("unexpected schema: %v", ci.TableSchema.Columns())
	}
	var errs []error
	xtras, err := parser_blank.ExtractValue[map[string]string](ci, parser_blank.ExtrasColumn)
	errs = append(errs, err)
	partition, err := parser_blank.ExtractValue[string](ci, parser_blank.PartitionColum)
	errs = append(errs, err)
	seqNo, err := parser_blank.ExtractValue[uint64](ci, parser_blank.SeqNoColumn)
	errs = append(errs, err)
	cTime, err := parser_blank.ExtractValue[time.Time](ci, parser_blank.CreateTimeColumn)
	errs = append(errs, err)
	wTime, err := parser_blank.ExtractValue[time.Time](ci, parser_blank.WriteTimeColumn)
	errs = append(errs, err)
	rawData, err := parser_blank.ExtractValue[[]byte](ci, parser_blank.RawMessageColumn)
	errs = append(errs, err)
	sourceID, err := parser_blank.ExtractValue[string](ci, parser_blank.SourceIDColumn)
	errs = append(errs, err)
	if err := errors.Join(errs...); err != nil {
		return msg, part, xerrors.Errorf("format errors: %w", err)
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
			Topic:     part.Topic,
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
	if schema != parser_blank.BlankSchema {
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
	p, err := parser_json.NewParserJSON(cfg.Parser, false, lgr, stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())))
	if err != nil {
		return nil, xerrors.Errorf("unable to construct json parser: %w", err)
	}
	wrappedParser, ok := p.(*parsers.ResourceableParser)
	if !ok {
		return nil, xerrors.Errorf("unknown parser: %T, must be wrapper", p)
	}
	genParser, ok := wrappedParser.Unwrap().(*generic_parser.GenericParser)
	if !ok {
		return nil, xerrors.Errorf("unknown parser: %T, must be generic parser", p)
	}
	return &parser{
		topic:  cfg.Topic,
		parser: genParser,
	}, nil
}
