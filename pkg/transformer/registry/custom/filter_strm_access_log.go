package custom

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/transformer"
	"go.ytsaurus.tech/library/go/core/log"
)

const FilterStrmAccessLogTransformerType = abstract.TransformerType("filter_strm_access_log")

func init() {
	transformer.Register[Config](
		FilterStrmAccessLogTransformerType,
		func(protoConfig Config, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
			return NewFilterStrmAccessLog(lgr), nil
		},
	)
}

type Config struct {
}

type FilterStrmAccessLog struct {
	Logger log.Logger
}

var neededTiers = map[string]bool{
	"vh-dzen-ugc-converted":      true,
	"vh-dzen-ugc-news-converted": true,
	"vh-lkpo-converted":          true,
	"vh-news-converted":          true,
	"vh-ugc-converted":           true,
	"vh-youtube-converted":       true,
	"vh-zen-converted":           true,
	"vh-zen-testing-converted":   true,
	"zen-vod":                    true,
}

func optimizedColumnNameIndex(changeItem *abstract.ChangeItem, colName string, prevIndex int) int {
	if len(changeItem.ColumnNames) > prevIndex && changeItem.ColumnNames[prevIndex] == colName {
		return prevIndex
	} else {
		return changeItem.ColumnNameIndex(colName)
	}
}

func (f *FilterStrmAccessLog) Type() abstract.TransformerType {
	return FilterStrmAccessLogTransformerType
}

func (f *FilterStrmAccessLog) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	result := make([]abstract.ChangeItem, 0, len(input))
	trafficTypeIndex := 0
	unistatTierIndex := 0
	for _, changeItem := range input {
		if changeItem.Kind != abstract.InsertKind {
			result = append(result, changeItem)
			continue
		}

		//---

		trafficTypeIndex = optimizedColumnNameIndex(&changeItem, "traffic_type", trafficTypeIndex)
		if trafficTypeIndex == -1 {
			continue
		}
		if changeItem.ColumnValues[trafficTypeIndex] == nil {
			continue
		}
		var trafficType string
		switch t := changeItem.ColumnValues[trafficTypeIndex].(type) {
		case string:
			trafficType = t
		default:
			continue
		}

		//---

		unistatTierIndex = optimizedColumnNameIndex(&changeItem, "unistat_tier", unistatTierIndex)
		if unistatTierIndex == -1 {
			continue
		}
		if changeItem.ColumnValues[unistatTierIndex] == nil {
			continue
		}
		var unistatTier string
		switch t := changeItem.ColumnValues[unistatTierIndex].(type) {
		case string:
			unistatTier = t
		default:
			continue
		}

		//---

		if trafficType == "users" && neededTiers[unistatTier] {
			result = append(result, changeItem)
		}
	}
	return abstract.TransformerResult{
		Transformed: result,
		Errors:      nil,
	}
}

func (f *FilterStrmAccessLog) Suitable(_ abstract.TableID, _ *abstract.TableSchema) bool {
	return true
}

func (f *FilterStrmAccessLog) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	return original, nil
}

func (f *FilterStrmAccessLog) Description() string {
	return "Filter strm-access-log"
}

func NewFilterStrmAccessLog(lgr log.Logger) *FilterStrmAccessLog {
	return &FilterStrmAccessLog{
		Logger: lgr,
	}
}
