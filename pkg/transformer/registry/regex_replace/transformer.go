package regexreplace

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/transformer"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
)

const Type = abstract.TransformerType("regex_replace_transformer")

func init() {
	transformer.Register[Config](Type, func(cfg Config, logger log.Logger, _ abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
		matchRe, err := regexp.Compile(cfg.RegexMatch)
		if err != nil {
			return nil, fmt.Errorf("unable to compile match regexp: %w", err)
		}

		clms, err := filter.NewFilter(cfg.Columns.IncludeColumns, cfg.Columns.ExcludeColumns)
		if err != nil {
			return nil, fmt.Errorf("unable to create columns filter: %w", err)
		}

		tbls, err := filter.NewFilter(cfg.Tables.IncludeTables, cfg.Tables.ExcludeTables)
		if err != nil {
			return nil, fmt.Errorf("unable to create tables filter: %w", err)
		}

		return &Transformer{
			MatchRegex:  matchRe,
			ReplaceRule: cfg.ReplaceRule,
			Columns:     clms,
			Tables:      tbls,
			Logger:      logger,
		}, nil
	})
}

type Config struct {
	RegexMatch  string         `json:"regexMatch"`
	ReplaceRule string         `json:"replaceRule"`
	Columns     filter.Columns `json:"columns"`
	Tables      filter.Tables  `json:"tables"`
}

type Transformer struct {
	MatchRegex  *regexp.Regexp
	ReplaceRule string

	Columns filter.Filter
	Tables  filter.Filter
	Logger  log.Logger
}

func (t *Transformer) Type() abstract.TransformerType {
	return Type
}

func (t *Transformer) Suitable(table abstract.TableID, _ *abstract.TableSchema) bool {
	return t.Tables.Match(table.Name)
}

func (t *Transformer) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	return original, nil
}

func (t *Transformer) Description() string {
	if t.Columns.Empty() {
		return "Replace all string column values via regular expression"
	}

	includeStr := ctrcut(strings.Join(t.Columns.IncludeRegexp, "|"), 100)
	excludeStr := ctrcut(strings.Join(t.Columns.ExcludeRegexp, "|"), 100)
	return fmt.Sprintf("Replace given string column values (include: %s, exclude: %s) via regular expression `%s`", includeStr, excludeStr, t.MatchRegex)
}

func ctrcut(value string, maxLength int) string {
	return value[:min(maxLength, len(value))]
}

func (t *Transformer) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	if t.MatchRegex == nil {
		return abstract.TransformerResult{
			Transformed: input,
			Errors:      nil,
		}
	}

	transformed := make([]abstract.ChangeItem, 0, len(input))

	for _, item := range input {
		if !t.Tables.Match(item.Table) {
			continue
		}

		newValues := make([]any, len(item.ColumnValues))

		for i, columnName := range item.ColumnNames {
			if !t.Columns.Match(columnName) {
				newValues[i] = item.ColumnValues[i]
				continue
			}

			val := item.ColumnValues[i]
			typ := item.TableSchema.Columns()[i].DataType

			newValues[i] = replace(val, typ, t.MatchRegex, t.ReplaceRule)
		}

		item.ColumnValues = newValues
		transformed = append(transformed, item)
	}

	return abstract.TransformerResult{
		Transformed: transformed,
		Errors:      nil,
	}
}

// replace performs regular expression replace.
// It only works for YT types `string` and `utf8`
func replace(value any, typ string, match *regexp.Regexp, replace string) any {
	switch typ {
	case schema.TypeString.String():
		out, ok := value.(string)
		if ok {
			return match.ReplaceAllString(out, replace)
		}
	case schema.TypeBytes.String():
		out, ok := value.([]byte)
		if ok {
			return match.ReplaceAll(out, []byte(replace))
		}
	}

	return value
}
