package filterrowsbyids

import (
	"errors"

	"github.com/jackc/pgtype"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/transformer"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	"go.ytsaurus.tech/library/go/core/log"
	yts "go.ytsaurus.tech/yt/go/schema"
)

const Type = abstract.TransformerType("filter_rows_by_ids")

func init() {
	transformer.Register[Config](Type, func(cfg Config, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
		return NewFilterRowsByIDsTransformer(cfg, lgr)
	})
}

type Config struct {
	Tables     filter.Tables  `json:"tables"`
	Columns    filter.Columns `json:"columns"`
	AllowedIDs []string       `json:"allowedIDs"`
}

type FilterRowsByIDsTransformer struct {
	Tables     filter.Filter
	Columns    filter.Filter
	AllowedIDs map[string]struct{}
	Logger     log.Logger
}

func NewFilterRowsByIDsTransformer(cfg Config, lgr log.Logger) (*FilterRowsByIDsTransformer, error) {
	tbls, err := filter.NewFilter(cfg.Tables.IncludeTables, cfg.Tables.ExcludeTables)
	if err != nil {
		return nil, xerrors.Errorf("unable to create tables filter: %w", err)
	}
	cols, err := filter.NewFilter(cfg.Columns.IncludeColumns, cfg.Columns.ExcludeColumns)
	if err != nil {
		return nil, xerrors.Errorf("unable to create columns filter: %w", err)
	}
	if len(cfg.AllowedIDs) == 0 {
		return nil, errors.New("list of allowed IDs shouldn't be empty")
	}

	allowedIDsSet := make(map[string]struct{}, len(cfg.AllowedIDs))
	for _, allowedID := range cfg.AllowedIDs {
		allowedIDsSet[allowedID] = struct{}{}
	}

	return &FilterRowsByIDsTransformer{
		Tables:     tbls,
		Columns:    cols,
		AllowedIDs: allowedIDsSet,
		Logger:     lgr,
	}, nil
}

func (t *FilterRowsByIDsTransformer) Type() abstract.TransformerType {
	return Type
}

func (t *FilterRowsByIDsTransformer) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	transformed := make([]abstract.ChangeItem, 0)
	for _, item := range input {
		if t.shouldKeep(item) {
			transformed = append(transformed, item)
		}
	}

	return abstract.TransformerResult{
		Transformed: transformed,
		Errors:      []abstract.TransformerError{},
	}
}

func (t *FilterRowsByIDsTransformer) shouldKeep(item abstract.ChangeItem) bool {
	item_as_map := item.AsMap()
	for _, colSchema := range item.TableSchema.Columns() {
		if !t.checkColumnSuitable(colSchema) {
			continue
		}

		var asString string

		switch castedValue := item_as_map[colSchema.ColumnName].(type) {
		case string:
			asString = castedValue
		case *pgtype.GenericText:
			asString = castedValue.String
		case *pgtype.Text:
			asString = castedValue.String
		case *pgtype.Varchar:
			asString = castedValue.String
		default:
			continue
		}

		if _, ok := t.AllowedIDs[asString]; ok {
			return true
		}
	}

	return false
}

func (t *FilterRowsByIDsTransformer) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	if !filter.MatchAnyTableNameVariant(t.Tables, table) {
		return false
	}

	for _, colSchema := range schema.Columns() {
		if t.checkColumnSuitable(colSchema) {
			return true
		}
	}

	return false
}

func (t *FilterRowsByIDsTransformer) checkColumnSuitable(colSchema abstract.ColSchema) bool {
	if colSchema.DataType != yts.TypeString.String() && colSchema.DataType != yts.TypeBytes.String() && colSchema.DataType != yts.TypeAny.String() {
		return false
	}

	if t.Columns.Empty() {
		return true
	}

	return t.Columns.Match(colSchema.ColumnName)
}

func (t *FilterRowsByIDsTransformer) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	return original, nil
}

func (t *FilterRowsByIDsTransformer) Description() string {
	return "Row filter by allowed IDs"
}
