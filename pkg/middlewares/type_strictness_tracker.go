package middlewares

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/typesystem/values"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
)

// TypeStrictnessTracker tracks the "strictness" of types passing through it.
//
// "Strictness" is the compliance of the column value in an item with the *strict* representation defined by the Data Transfer type system.
func TypeStrictnessTracker(logger log.Logger, stats *stats.TypeStrictnessStats) func(abstract.Sinker) abstract.Sinker {
	return func(s abstract.Sinker) abstract.Sinker {
		return newTypeStrictnessTracker(s, logger, stats)
	}
}

type typeStrictnessTracker struct {
	sink abstract.Sinker

	stats   *stats.TypeStrictnessStats
	checker *strictnessChecker

	logger log.Logger
}

func newTypeStrictnessTracker(s abstract.Sinker, logger log.Logger, stats *stats.TypeStrictnessStats) *typeStrictnessTracker {
	result := &typeStrictnessTracker{
		sink: s,

		stats:   stats,
		checker: newStrictnessChecker(),

		logger: logger,
	}
	return result
}

func (t *typeStrictnessTracker) Close() error {
	return t.sink.Close()
}

func (t *typeStrictnessTracker) Push(input []abstract.ChangeItem) error {
	for i := range input {
		if t.checker.HasNonStrictTypes(&input[i]) {
			t.stats.Bad.Inc()
		} else {
			t.stats.Good.Inc()
		}
	}
	return t.sink.Push(input)
}

// strictnessChecker caches the mapping of column name to its schema for each table it has seen.
//
// This object exists only for optimization purposes. ChangeItem could have a different structure - where each value has an "attached" table schema - but this requires lots of refactoring which cannot be done easily.
type strictnessChecker struct {
	tableSchemas map[abstract.TableID]abstract.FastTableSchema

	strictValueTypeChecks map[schema.Type]values.ValueTypeChecker
}

func newStrictnessChecker() *strictnessChecker {
	return &strictnessChecker{
		tableSchemas: make(map[abstract.TableID]abstract.FastTableSchema),

		strictValueTypeChecks: values.StrictValueTypeCheckers(),
	}
}

// HasNonStrictTypes checks whether an item contain types which do not fulfill the strictness guarantee
func (c *strictnessChecker) HasNonStrictTypes(item *abstract.ChangeItem) bool {
	tSchema, ok := c.tableSchemas[item.TableID()]
	if !ok {
		if len(item.TableSchema.Columns()) == 0 {
			return false
		}
		c.tableSchemas[item.TableID()] = abstract.MakeFastTableSchema(item.TableSchema.Columns())
		tSchema = c.tableSchemas[item.TableID()]
	}

	if !item.IsRowEvent() {
		return false
	}

	for i := range item.ColumnNames {
		colSchema, ok := tSchema[abstract.ColumnName(item.ColumnNames[i])]
		if !ok {
			continue
		}
		colType := schema.Type(colSchema.DataType)
		check, ok := c.strictValueTypeChecks[colType]
		if !ok {
			continue
		}
		if !check(item.ColumnValues[i]) {
			return true
		}
	}
	return false
}
