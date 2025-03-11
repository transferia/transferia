package middlewares

import (
	"github.com/transferria/transferria/library/go/core/metrics"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/stats"
)

type ExcludePredicate func(*abstract.ChangeItem) bool

func ExcludeSystemTables(item *abstract.ChangeItem) bool { return item.IsSystemTable() }

func Filter(registry metrics.Registry, predicates ...ExcludePredicate) func(abstract.Sinker) abstract.Sinker {
	return func(s abstract.Sinker) abstract.Sinker {
		return newFilter(s, registry, predicates)
	}
}

type filter struct {
	predicates []ExcludePredicate
	downstream abstract.Sinker

	sta *stats.MiddlewareFilterStats
}

func newFilter(downstream abstract.Sinker, registry metrics.Registry, predicates []ExcludePredicate) *filter {
	return &filter{
		predicates: predicates,
		downstream: downstream,

		sta: stats.NewMiddlewareFilterStats(registry),
	}
}

func (f *filter) Close() error {
	return f.downstream.Close()
}

func (f *filter) shouldExcludeItem(item *abstract.ChangeItem) bool {
	for _, predicate := range f.predicates {
		if predicate(item) {
			return true
		}
	}
	return false
}

func (f *filter) filterBatch(items []abstract.ChangeItem) []abstract.ChangeItem {
	filteredBatch := make([]abstract.ChangeItem, 0, len(items))
	for _, item := range items {
		if f.shouldExcludeItem(&item) {
			f.sta.Dropped.Inc()
			continue
		}
		filteredBatch = append(filteredBatch, item)
	}
	return filteredBatch
}

func (f *filter) Push(items []abstract.ChangeItem) error {
	haveAnythingToExclude := false
	for i := range items {
		if f.shouldExcludeItem(&items[i]) {
			haveAnythingToExclude = true
			break
		}
	}

	// Do not copy the entire batch unless there is at least one filtered item
	if haveAnythingToExclude {
		items = f.filterBatch(items)
	}
	if len(items) == 0 {
		return nil
	}
	return f.downstream.Push(items)
}
