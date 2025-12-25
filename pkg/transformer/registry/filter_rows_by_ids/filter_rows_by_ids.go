package filterrowsbyids

import (
	"errors"

	"github.com/jackc/pgtype"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/mongo"
	"github.com/transferia/transferia/pkg/transformer"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	"go.ytsaurus.tech/library/go/core/log"
	yts "go.ytsaurus.tech/yt/go/schema"
)

const Type = abstract.TransformerType("filter_rows_by_ids")

const document = "document"

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
	Tables  filter.Filter
	Columns filter.Filter
	// prefixes length -> set of prefixes
	// Optimized specifically for use by Taxi team, where we want to check
	// whether column values start with allowed IDs.
	// Since most IDs have the same length, it's efficient to group them by
	// length in maps and then check substrings of column values.
	AllowedIDPrefixesByLength map[int]map[string]struct{}
	Logger                    log.Logger
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

	allowedIDPrefixesByLength := make(map[int]map[string]struct{})
	for _, allowedID := range cfg.AllowedIDs {
		allowedIDPrefixes, ok := allowedIDPrefixesByLength[len(allowedID)]
		if !ok {
			allowedIDPrefixes = make(map[string]struct{})
			allowedIDPrefixesByLength[len(allowedID)] = allowedIDPrefixes
		}

		allowedIDPrefixes[allowedID] = struct{}{}
	}

	return &FilterRowsByIDsTransformer{
		Tables:                    tbls,
		Columns:                   cols,
		AllowedIDPrefixesByLength: allowedIDPrefixesByLength,
		Logger:                    lgr,
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
	itemAsMap := item.AsMap()
	for _, colSchema := range item.TableSchema.Columns() {
		if t.isMongoDocumentColumn(colSchema) {
			if t.processMongoDocument(itemAsMap[document]) {
				return true
			}

			continue
		}

		if !t.checkColumnSuitable(colSchema) {
			continue
		}

		var asString string

		switch castedValue := itemAsMap[colSchema.ColumnName].(type) {
		case string:
			asString = castedValue
		case []byte:
			asString = string(castedValue)
		case *pgtype.GenericText:
			asString = castedValue.String
		case *pgtype.Text:
			asString = castedValue.String
		case *pgtype.Varchar:
			asString = castedValue.String
		default:
			continue
		}

		if t.isIDAllowed(asString) {
			return true
		}
	}

	return false
}

func (t *FilterRowsByIDsTransformer) processMongoDocument(value interface{}) bool {
	dValue, ok := value.(mongo.DValue)
	if !ok {
		return false
	}

	for _, elem := range dValue.D {
		if !t.Columns.Match(elem.Key) {
			continue
		}

		asString, ok := elem.Value.(string)
		if !ok {
			continue
		}

		if t.isIDAllowed(asString) {
			return true
		}
	}

	return false
}

func (t *FilterRowsByIDsTransformer) isIDAllowed(ID string) bool {
	for prefixesLength, prefixes := range t.AllowedIDPrefixesByLength {
		if prefixesLength > len(ID) {
			continue
		}

		if _, ok := prefixes[ID[0:prefixesLength]]; ok {
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
		// To filter rows by nested fields in BSON document, we always consider
		// `document` column in Mongo collections as suitable.
		if t.checkColumnSuitable(colSchema) || t.isMongoDocumentColumn(colSchema) {
			return true
		}
	}

	return false
}

func (t *FilterRowsByIDsTransformer) isMongoDocumentColumn(colSchema abstract.ColSchema) bool {
	return colSchema.ColumnName == document && colSchema.DataType == yts.TypeAny.String()
}

func (t *FilterRowsByIDsTransformer) checkColumnSuitable(colSchema abstract.ColSchema) bool {
	if colSchema.DataType != yts.TypeString.String() && colSchema.DataType != yts.TypeBytes.String() && colSchema.DataType != yts.TypeAny.String() {
		return false
	}

	return t.Columns.Match(colSchema.ColumnName)
}

func (t *FilterRowsByIDsTransformer) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	return original, nil
}

func (t *FilterRowsByIDsTransformer) Description() string {
	return "Row filter by allowed IDs"
}
