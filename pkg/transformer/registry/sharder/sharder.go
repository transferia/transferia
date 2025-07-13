package sharder

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/transformer"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	tostring "github.com/transferia/transferia/pkg/transformer/registry/to_string"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

const Type = abstract.TransformerType("sharder_transformer")

func init() {
	transformer.Register[Config](Type, func(cfg Config, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
		tbls, err := filter.NewFilter(cfg.Tables.IncludeTables, cfg.Tables.ExcludeTables)
		if err != nil {
			return nil, xerrors.Errorf("unable to create tables filter: %w", err)
		}

		var columns filter.Filter
		isRandom := false
		if cfg.IsRandom {
			isRandom = true
		} else {
			columns, err = filter.NewFilter(cfg.Columns.IncludeColumns, cfg.Columns.ExcludeColumns)
			if err != nil {
				return nil, xerrors.Errorf("unable to create columns filter: %w", err)
			}
		}

		shardsNum, err := strconv.ParseInt(cfg.ShardsCount, 10, 64)
		if err != nil {
			return nil, xerrors.Errorf("cannot parse param as int: %w", err)
		}
		return &SharderTransformer{
			Logger: lgr,

			// config part
			Tables:    tbls,
			Columns:   columns,
			IsRandom:  isRandom,
			ShardsNum: shardsNum,

			uuidForRandom: uuid.New().String(),
		}, nil
	})
}

type Config struct {
	Tables filter.Tables `json:"tables"`

	// 'Columns' and 'IsRandom' is mutually exclusive
	Columns  filter.Columns `json:"columns"`
	IsRandom bool           `json:"is_random"`

	ShardsCount string `json:"shardsCount"` // it's string type, bcs of compatibility - long long time ago it was string
}

type SharderTransformer struct {
	Logger log.Logger

	// config part
	Tables    filter.Filter
	Columns   filter.Filter
	IsRandom  bool
	ShardsNum int64

	uuidForRandom string
}

func (f *SharderTransformer) Type() abstract.TransformerType {
	return Type
}

func (f *SharderTransformer) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	transformed := make([]abstract.ChangeItem, 0, len(input))
	for _, item := range input {
		item.PartID = f.generatePartID(&item)
		transformed = append(transformed, item)
	}
	return abstract.TransformerResult{
		Transformed: transformed,
		Errors:      nil,
	}
}

func (f *SharderTransformer) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	if !filter.MatchAnyTableNameVariant(f.Tables, table) {
		return false
	}
	if f.Columns.Empty() {
		return true
	}
	for _, colSchema := range schema.Columns() {
		if f.Columns.Match(colSchema.ColumnName) {
			return true
		}
	}
	return false
}

func (f *SharderTransformer) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	return original, nil
}

func (f *SharderTransformer) Description() string {
	if f.Columns.Empty() {
		return "Transform to shard tables by field values"
	}
	includeStr := trimStr(strings.Join(f.Columns.IncludeRegexp, "|"), 100)
	excludeStr := trimStr(strings.Join(f.Columns.ExcludeRegexp, "|"), 100)
	return fmt.Sprintf("Transform to shard tables by field values (include: %s, exclude: %s, shards_num: %d)", includeStr, excludeStr, f.ShardsNum)
}

func trimStr(value string, maxLength int) string {
	if len(value) > maxLength {
		value = value[:maxLength]
	}
	return value
}

func (f *SharderTransformer) generatePartID(item *abstract.ChangeItem) string {
	if f.IsRandom {
		return fmt.Sprintf("%s_%d", f.uuidForRandom, rand.Intn(int(f.ShardsNum)))
	} else {
		arrValues := make([]string, 0)
		fieldNameToVal := item.AsMap()
		for i := range item.TableSchema.Columns() {
			currColumnName := item.TableSchema.Columns()[i].ColumnName
			if f.Columns.Match(currColumnName) {
				arrValues = append(arrValues, tostring.SerializeToString(fieldNameToVal[currColumnName], item.TableSchema.Columns()[i].DataType))
			}
		}
		summaryStr := strings.Join(arrValues, ".") // we're not afraid of cases {'a.b', 'c'} vs {'a', 'b.c'} here - they just will get into same shard
		return fmt.Sprintf("%d", util.CRC32FromString(summaryStr)%uint32(f.ShardsNum))
	}
}
