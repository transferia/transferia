package filter

import (
	"fmt"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/transformer"
	"github.com/transferria/transferria/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
)

const SkipEventsTransformerType = abstract.TransformerType("skip_events")

func init() {
	transformer.Register[SkipEventsConfig](
		SkipEventsTransformerType,
		func(protoConfig SkipEventsConfig, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
			return NewSkipEvents(protoConfig, lgr)
		},
	)
}

type SkipEventsConfig struct {
	Tables Tables   `json:"tables"`
	Events []string `json:"events"`
}

type SkipEvents struct {
	tables Filter
	kinds  *set.Set[abstract.Kind]
}

func NewSkipEvents(transformer SkipEventsConfig, logger log.Logger) (*SkipEvents, error) {
	tables, err := NewFilter(transformer.Tables.IncludeTables, transformer.Tables.ExcludeTables)
	if err != nil {
		return nil, xerrors.Errorf("unable to init table filter: %w", err)
	}

	kinds := set.New[abstract.Kind]()
	for _, eventType := range transformer.Events {
		kinds.Add(abstract.Kind(eventType))
	}

	return &SkipEvents{tables: tables, kinds: kinds}, nil
}

func (se *SkipEvents) Type() abstract.TransformerType {
	return SkipEventsTransformerType
}

func (se *SkipEvents) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	var output []abstract.ChangeItem
	for _, item := range input {
		if !se.kinds.Contains(item.Kind) {
			output = append(output, item)
		}
	}
	var result abstract.TransformerResult
	result.Transformed = output
	return result
}

func (se *SkipEvents) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	return MatchAnyTableNameVariant(se.tables, table)
}

func (se *SkipEvents) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	return original, nil
}

func (se *SkipEvents) Description() string {
	return fmt.Sprintf("skips the following event types: %v", se.kinds)
}
