package problemitemdetector

import (
	"encoding/json"
	"fmt"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/transformer"
	"go.ytsaurus.tech/library/go/core/log"
)

func PluggableProblemItemTransformer(transfer *model.Transfer, _ metrics.Registry, _ coordinator.Coordinator) func(abstract.Sinker) abstract.Sinker {
	if transfer.Transformation == nil || transfer.Transformation.Transformers == nil {
		return middlewares.IdentityMiddleware
	}

	applicable := transferNeedDetector(transfer.Transformation.Transformers)
	if !applicable {
		return middlewares.IdentityMiddleware
	}

	return func(s abstract.Sinker) abstract.Sinker {
		return newPluggableTransformer(s)
	}
}

func transferNeedDetector(transformers *transformer.Transformers) bool {
	for _, t := range transformers.Transformers {
		if v, ok := t[TransformerType]; ok {
			_, applicable := v.(Config)
			return applicable
		}
	}
	return false
}

type pluggableTransformer struct {
	sink abstract.Sinker
}

func newPluggableTransformer(s abstract.Sinker) abstract.Sinker {
	return &pluggableTransformer{s}
}

func (d *pluggableTransformer) Close() error {
	return d.sink.Close()
}

func (d *pluggableTransformer) Push(items []abstract.ChangeItem) error {
	err := d.sink.Push(items)
	if err != nil {
		return d.pushProblemItemsSlice(items, err)
	}
	return nil
}

func (d *pluggableTransformer) pushProblemItemsSlice(items []abstract.ChangeItem, logError error) error {
	for i := range items {
		if err := d.sink.Push([]abstract.ChangeItem{items[i]}); err != nil {
			return d.logProblemItem(items[i], logError)
		}
	}

	return nil
}

func (d *pluggableTransformer) logProblemItem(item abstract.ChangeItem, logError error) error {
	logger.Log.Error(fmt.Sprintf("problem_item_detector - found problem item, table '%s'", item.Fqtn()), log.Error(logError))

	// to avoid problem with len of log convert the values and log them separately
	for i, value := range item.ColumnValues {
		if encodedVal, err := json.Marshal(item.ColumnValues[i]); err == nil {
			logger.Log.Errorf("problem_item_detector - type: %T, column %s : %s", value, item.ColumnNames[i], string(encodedVal))
		} else {
			logger.Log.Errorf("problem_item_detector (unable marshal value) - type: %T, column %s : %v", value, item.ColumnNames[i], value)
		}
	}
	for i, value := range item.OldKeys.KeyValues {
		if encodedVal, err := json.Marshal(item.OldKeys.KeyValues[i]); err == nil {
			logger.Log.Errorf("problem_item_detector - OldKeys type: %T, column %s : %s", value, item.OldKeys.KeyNames[i], string(encodedVal))
		} else {
			logger.Log.Errorf("problem_item_detector (unable marshal value) - OldKeys type: %T, column %s : %v", value, item.OldKeys.KeyNames[i], value)
		}
	}

	return abstract.NewFatalError(xerrors.New("bad item detector found problem item"))
}

func init() {
	middlewares.PlugTransformer(PluggableProblemItemTransformer)
}
