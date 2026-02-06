package transformer

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
)

const transformErrorColumn string = "__transform_error"

type transformation struct {
	config       *Transformers
	transformers []abstract.Transformer
	plan         map[abstract.TableID]map[string][]abstract.Transformer
	sink         abstract.Sinker

	registry metrics.Registry
	sta      *stats.MiddlewareTransformerStats
	logger   log.Logger

	mutex sync.Mutex

	runtimeOpts abstract.TransformationRuntimeOpts
}

func (u *transformation) clone() *transformation {
	return &transformation{
		config:       u.config,
		transformers: u.transformers,
		plan:         make(map[abstract.TableID]map[string][]abstract.Transformer),
		sink:         nil,

		registry: u.registry,
		sta:      stats.NewMiddlewareTransformerStats(u.registry),
		logger:   u.logger,

		mutex: sync.Mutex{},

		runtimeOpts: u.runtimeOpts,
	}
}

func (u *transformation) AddTablePlan(table abstract.TableID, schema *abstract.TableSchema) ([]abstract.Transformer, error) {
	tablePlan := make([]abstract.Transformer, 0)
	inputSchemaHash, err := schema.Hash()
	if err != nil {
		u.logger.Warnf("unable to get table schema hash: %v", err)
	}
	var errs util.Errors
	outputSchema := schema
	for _, tr := range u.transformers {
		if tr.Suitable(table, outputSchema) {
			tablePlan = append(tablePlan, tr)
			resSchema, err := tr.ResultSchema(outputSchema)
			if err != nil {
				errs = util.AppendErr(errs, abstract.NewFatalError(xerrors.Errorf("unable to build result schema for: %s: %w", tr.Description(), err)))
				continue
			}
			outputSchema = resSchema
		} else {
			u.logger.Infof("transformer: %s not for table %v", tr.Description(), table.Fqtn())
		}
	}
	if _, ok := u.plan[table]; !ok {
		u.plan[table] = make(map[string][]abstract.Transformer)
	}
	u.plan[table][inputSchemaHash] = tablePlan
	if len(tablePlan) > 0 {
		u.logger.Infof(
			"set transformation plan for table %v\ninput schema: %v\nresult schema: %v",
			u.printfPlan(table, tablePlan),
			u.printfSchema(schema.Columns()),
			u.printfSchema(outputSchema.Columns()),
		)
	} else {
		u.logger.Infof("no transformations for table %v", table.Fqtn())
	}
	if !errs.Empty() {
		return nil, xerrors.Errorf("fail to add table: %s plan: %w", table.String(), errs)
	}
	return tablePlan, nil
}

func (u *transformation) printfPlan(tableID abstract.TableID, transformers []abstract.Transformer) string {
	str := fmt.Sprintf("%v:\n", tableID.Fqtn())
	for i, tr := range transformers {
		str += fmt.Sprintf("\t%v:\t%v\n", i, tr.Description())
	}
	return str
}

func (u *transformation) preparePlans(items []abstract.ChangeItem) (map[abstract.TableID]map[string][]abstract.Transformer, error) {
	u.mutex.Lock()
	defer u.mutex.Unlock()

	result := map[abstract.TableID]map[string][]abstract.Transformer{}

	var errs util.Errors
	var err error
	for _, item := range items {
		tableID := item.TableID()
		schemaHash, _ := item.TableSchema.Hash()
		if _, ok := result[tableID]; !ok {
			result[tableID] = make(map[string][]abstract.Transformer)
		}
		if _, ok := result[tableID][schemaHash]; ok {
			continue
		}
		tablePlan, ok := u.plan[tableID][schemaHash]
		if !ok {
			tablePlan, err = u.AddTablePlan(tableID, item.TableSchema)
			if err != nil {
				errs = util.AppendErr(errs, xerrors.Errorf("unable to add table plan: %w", err))
			}
		}
		result[tableID][schemaHash] = append(result[tableID][schemaHash], tablePlan...)
	}
	if !errs.Empty() {
		return nil, xerrors.Errorf("prepare transformer plan failed: %w", errs)
	}
	return result, nil
}

func (u *transformation) Push(items []abstract.ChangeItem) error {
	itemsIncomingCount := len(items)
	startMoment := time.Now()

	plans, err := u.preparePlans(items)
	if err != nil {
		return xerrors.Errorf("unable to prepare transformation: %w", err)
	}
	tableItems := abstract.SplitByTableID(items)
	result := make(chan chan abstract.TransformerResult, len(plans))

	for tid, plan := range plans {
		resICh := make(chan abstract.TransformerResult)
		result <- resICh
		go u.do(tid, plan, tableItems[tid], resICh)
	}

	transformed := make([]abstract.ChangeItem, 0)
	errors := make([]abstract.TransformerError, 0)
	for i := 0; i < len(plans); i++ {
		iRes := <-<-result
		transformed = append(transformed, iRes.Transformed...)
		errors = append(errors, iRes.Errors...)
	}
	close(result)

	elapsed := time.Since(startMoment)
	itemsTransformedCount := len(transformed)
	itemsDroppedCount := itemsIncomingCount - itemsTransformedCount

	if itemsDroppedCount > 0 {
		u.sta.Dropped.Add(int64(itemsDroppedCount))
	}
	u.sta.Elapsed.RecordDuration(elapsed)
	u.sta.Errors.Add(int64(len(errors)))

	u.logFormatStringIfErrors(errors, "transformation of %d plans applied in %s; converted %d into %d items (%+d items), %d errors", len(plans), elapsed.String(), itemsIncomingCount, itemsTransformedCount, -itemsDroppedCount, len(errors))

	if len(errors) > 0 {
		if err := u.pushErrors(errors); err != nil {
			return xerrors.Errorf("failed to process transformation errors: %w", err)
		}
	}

	return u.sink.Push(transformed)
}

func (u *transformation) Close() error {
	if u.sink != nil {
		return u.sink.Close()
	}
	return nil
}

func (u *transformation) logFormatStringIfErrors(errors []abstract.TransformerError, msg string, args ...any) {
	lgr := u.logger.Debugf
	if len(errors) > 0 {
		lgr = u.logger.Warnf
	}
	lgr(msg, args...)
}

func (u *transformation) pushErrors(errors []abstract.TransformerError) error {
	if len(errors) == 0 {
		return nil
	}
	if u.config == nil || u.config.ErrorsOutput == nil {
		if err := u.sink.Push(errorChangeItems(errors)); err != nil {
			return xerrors.Errorf("failed to push untransformable (errorneous) items: %w", err)
		}
		return nil
	}
	switch u.config.ErrorsOutput.Type {
	case SinkErrorsOutput:
		if err := u.sink.Push(errorChangeItems(errors)); err != nil {
			return xerrors.Errorf("failed to push untransformable (errorneous) items: %w", err)
		}
	case DevnullErrorsOutput:
		u.logger.Warn("transformation ignores errors", log.Int("len", len(errors)))
		u.logger.Warnf("error sample: %v, \nitem: %s", errors[0].Error, errors[0].Input.ToJSONString())
		return nil
	default:
		return xerrors.Errorf("output format %s not implemented", u.config.ErrorsOutput.Type)
	}
	return nil
}

func errorChangeItems(errors []abstract.TransformerError) []abstract.ChangeItem {
	res := make([]abstract.ChangeItem, len(errors))
	for i, errRow := range errors {
		res[i] = abstract.ChangeItem{
			ID:           errRow.Input.ID,
			LSN:          errRow.Input.LSN,
			CommitTime:   errRow.Input.CommitTime,
			Counter:      errRow.Input.Counter,
			Kind:         errRow.Input.Kind,
			Schema:       errRow.Input.Schema,
			Table:        errRow.Input.Table,
			PartID:       errRow.Input.PartID,
			ColumnNames:  append(errRow.Input.ColumnNames, transformErrorColumn),
			ColumnValues: append(errRow.Input.ColumnValues, errRow.Error.Error()),
			TableSchema: abstract.NewTableSchema(append(errRow.Input.TableSchema.Columns(), abstract.ColSchema{
				TableSchema:  errRow.Input.Schema,
				TableName:    errRow.Input.Table,
				Path:         "",
				ColumnName:   transformErrorColumn,
				DataType:     string(schema.TypeString),
				PrimaryKey:   false,
				FakeKey:      false,
				Required:     false,
				Expression:   "",
				OriginalType: "",
				Properties:   nil,
			})),
			OldKeys: errRow.Input.OldKeys,
			Size: abstract.EventSize{
				Read:   0,
				Values: 0,
			},
			TxID:             errRow.Input.TxID,
			Query:            errRow.Input.Query,
			QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: "", PartitionNum: 0, Offset: 0, Index: 0},
		}
	}
	return res
}

func (u *transformation) do(tableID abstract.TableID, tablePlans map[string][]abstract.Transformer, items []abstract.ChangeItem, resCh chan abstract.TransformerResult) {
	result := abstract.TransformerResult{
		Transformed: make([]abstract.ChangeItem, 0),
		Errors:      make([]abstract.TransformerError, 0),
	}
	input := items
	u.logger.Debugf("for '%s' are '%v' plans to transform '%v' changeitems", tableID.String(), len(tablePlans), len(input))
	currentSchemaHash, _ := input[0].TableSchema.Hash()

	for i, lastIndex := 0, 0; i <= len(input); i++ {
		var hash string
		if i < len(input) {
			hash, _ = input[i].TableSchema.Hash()
			if currentSchemaHash == hash {
				continue
			}
		}
		toApply := input[lastIndex:i]

		for _, transformer := range tablePlans[currentSchemaHash] {
			st := time.Now()
			currentTransformerResult := transformer.Apply(toApply)

			toApply = currentTransformerResult.Transformed
			result.Errors = append(result.Errors, currentTransformerResult.Errors...)

			errorExample := ""
			if len(currentTransformerResult.Errors) != 0 {
				mapBytes, _ := json.Marshal(currentTransformerResult.Errors[0].Input.AsMap())
				errorExample = fmt.Sprintf("err:%s, changeItem:%s", currentTransformerResult.Errors[0].Error, mapBytes)
			}

			u.logFormatStringIfErrors(
				currentTransformerResult.Errors,
				"transformation plan applied for table '%s' - transformer '%s' - got %d items, transformed %d items with %d errors in %v milliseconds, err: %s",
				tableID.String(),
				transformer.Description(),
				len(toApply),
				len(currentTransformerResult.Transformed),
				len(currentTransformerResult.Errors),
				time.Since(st).Milliseconds(),
				errorExample,
			)
		}
		result.Transformed = append(result.Transformed, toApply...)
		if i < len(input) {
			lastIndex = i
			currentSchemaHash = hash
		}
	}
	resCh <- result
}

func (u *transformation) MakeSinkMiddleware() abstract.SinkOption {
	return func(sink abstract.Sinker) abstract.Sinker {
		executorInstance := u.clone()
		executorInstance.sink = sink
		return executorInstance
	}
}

func (u *transformation) AddTransformer(transformer abstract.Transformer) error {
	u.transformers = append(u.transformers, transformer)
	u.plan = make(map[abstract.TableID]map[string][]abstract.Transformer)
	return nil
}

func (u *transformation) printfSchema(schema abstract.TableColumns) string {
	res := fmt.Sprintf("Columns: %d\n", len(schema))
	for _, col := range schema {
		res += fmt.Sprintf("%s(%s) Key:%v\n", col.ColumnName, col.DataType, col.IsKey())
	}
	return res
}

func (u *transformation) RuntimeOpts() abstract.TransformationRuntimeOpts {
	return u.runtimeOpts
}

func Sinker(
	config *Transformers,
	runtime abstract.TransformationRuntimeOpts,
	transformers []abstract.Transformer,
	lgr log.Logger,
	registry metrics.Registry,
) abstract.SinkOption {
	return func(s abstract.Sinker) abstract.Sinker {
		tt := &transformation{
			config:       config,
			transformers: transformers,
			plan:         make(map[abstract.TableID]map[string][]abstract.Transformer),
			sink:         s,

			registry: registry,
			sta:      stats.NewMiddlewareTransformerStats(registry),
			logger:   lgr,

			mutex: sync.Mutex{},

			runtimeOpts: runtime,
		}
		return tt.MakeSinkMiddleware()(s)
	}
}
