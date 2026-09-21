package systemfields

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/transformer"
	transformer_filter "github.com/transferia/transferia/pkg/transformer/registry/filter"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	Type = abstract.TransformerType("system_fields_adder")
)

func init() {
	transformer.Register(Type,
		func(cfg Config, lgr log.Logger, _ abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
			return NewSystemFieldsAdder(cfg, lgr)
		},
	)
}

func isKindMatch(target abstract.Kind) bool {
	switch target {
	case abstract.InsertKind, abstract.UpdateKind:
		return true
	}
	return false
}

type Config struct {
	Tables transformer_filter.Tables
	Fields []SystemField
}

type SystemFieldsAdder struct {
	Tables transformer_filter.Filter
	Fields []SystemField
	Logger log.Logger
}

func NewSystemFieldsAdder(cfg Config, lgr log.Logger) (*SystemFieldsAdder, error) {
	tables, err := transformer_filter.NewFilter(cfg.Tables.IncludeTables, cfg.Tables.ExcludeTables)
	if err != nil {
		return nil, xerrors.Errorf("Unable to init table filter: %w", err)
	}
	return &SystemFieldsAdder{Tables: tables, Fields: cfg.Fields, Logger: lgr}, nil
}

func (t *SystemFieldsAdder) Type() abstract.TransformerType {
	return Type
}

func (t *SystemFieldsAdder) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	transformed := make([]abstract.ChangeItem, 0, len(input))
	for _, item := range input {
		isNameMatching := transformer_filter.MatchAnyTableNameVariant(t.Tables, item.TableID())
		if !isNameMatching || !isKindMatch(item.Kind) || abstract.IsSystemTable(item.TableID().Name) {
			transformed = append(transformed, item)
			continue
		}

		transformed = append(transformed, t.processItem(item))
	}

	return abstract.TransformerResult{Transformed: transformed, Errors: make([]abstract.TransformerError, 0)}
}

func (t *SystemFieldsAdder) processItem(item abstract.ChangeItem) abstract.ChangeItem {
	cols := item.TableSchema.Columns()
	for _, f := range t.Fields {
		cols = append(cols, abstract.NewColSchema(f.ColumnName, f.Type(), false))
		item.ColumnNames = append(item.ColumnNames, f.ColumnName)
		item.ColumnValues = append(item.ColumnValues, f.Value(item))
	}
	item.SetTableSchema(abstract.NewTableSchema(cols))
	return item
}

func (t *SystemFieldsAdder) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	return transformer_filter.MatchAnyTableNameVariant(t.Tables, table)
}

func (t *SystemFieldsAdder) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	cols := original.Columns()
	for _, f := range t.Fields {
		cols = append(cols, abstract.NewColSchema(f.ColumnName, f.Type(), false))
	}
	return abstract.NewTableSchema(cols), nil
}

func (t *SystemFieldsAdder) Description() string {
	return "Adds system fields from ChangeItems to its ColumnValues"
}
