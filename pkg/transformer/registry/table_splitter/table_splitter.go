package tablesplitter

import (
	"fmt"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers/generic"
	"github.com/transferia/transferia/pkg/transformer"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	tostring "github.com/transferia/transferia/pkg/transformer/registry/to_string"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	defaultSplitter = "/"
	Type            = abstract.TransformerType("table_splitter_transformer")
)

func init() {
	transformer.Register[Config](Type, func(cfg Config, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
		tbls, err := filter.NewFilter(cfg.Tables.IncludeTables, cfg.Tables.ExcludeTables)
		if err != nil {
			return nil, xerrors.Errorf("unable to create tables filter: %w", err)
		}
		return &TableSplitterTransformer{
			Columns:     cfg.Columns,
			Tables:      tbls,
			Splitter:    cfg.Splitter,
			UseLegacyLF: cfg.UseLegacyLF,
			Logger:      lgr,
		}, nil
	})
}

func GenerateTableName(originalTableName string, columns []string, splitter string, changeItem *abstract.ChangeItem) string {
	item := map[string]interface{}{}
	fastSchema := abstract.FastTableSchema{}
	if changeItem != nil {
		item = changeItem.AsMap()
		fastSchema = changeItem.TableSchema.FastColumns()
	}
	nameComponents := []string{}
	if originalTableName != "" {
		nameComponents = append(nameComponents, originalTableName)
	}
	if len(splitter) == 0 {
		splitter = defaultSplitter
	}
	for _, col := range columns {
		val := item[col]
		if sch, ok := fastSchema[abstract.ColumnName(col)]; ok {
			valAsString := tostring.SerializeToString(val, sch.DataType)
			nameComponents = append(nameComponents, valAsString)
		}
	}
	return strings.Join(nameComponents, splitter)
}

type Config struct {
	Tables      filter.Tables `json:"tables,omitempty"`
	Columns     []string      `json:"columns,omitempty"`
	Splitter    string        `json:"splitter,omitempty"`
	UseLegacyLF bool          `json:"useLegacyLf,omitempty"`
}

type TableSplitterTransformer struct {
	Tables      filter.Filter
	Columns     []string
	Splitter    string
	UseLegacyLF bool
	Logger      log.Logger
}

func (f *TableSplitterTransformer) Type() abstract.TransformerType {
	return Type
}

func (f *TableSplitterTransformer) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	transformed := make([]abstract.ChangeItem, 0, len(input))
	for _, item := range input {
		if f.UseLegacyLF {
			item.Table = generic.TableSplitter(item.Table, f.Columns, item.AsMap(), []interface{}{}, map[string]int{})
		} else {
			item.Table = GenerateTableName(item.Table, f.Columns, f.Splitter, &item)
		}
		transformed = append(transformed, item)
	}
	return abstract.TransformerResult{
		Transformed: transformed,
		Errors:      nil,
	}
}

func (f *TableSplitterTransformer) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	return filter.MatchAnyTableNameVariant(f.Tables, table)
}

func (f *TableSplitterTransformer) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	return original, nil
}

func (f *TableSplitterTransformer) Description() string {
	includeStr := trimStr(strings.Join(f.Tables.IncludeRegexp, "|"), 100)
	excludeStr := trimStr(strings.Join(f.Tables.ExcludeRegexp, "|"), 100)
	columns := trimStr(strings.Join(f.Tables.ExcludeRegexp, ","), 100)
	return fmt.Sprintf("Table splitter for tables=(include: %v, exclude: %v); columns=(%v); splitter=%v",
		includeStr, excludeStr, columns, f.Splitter)
}

func trimStr(value string, trimmedValueSize int) string {
	if len(value) < trimmedValueSize {
		trimmedValueSize = len(value)
	}
	trimmedVersion := fmt.Sprintf("%s... and %d more", value[:trimmedValueSize], len(value)-trimmedValueSize)
	if len(trimmedVersion) < len(value) {
		return trimmedVersion
	}
	return value
}
