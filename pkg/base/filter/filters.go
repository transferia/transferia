package filter

import (
	"github.com/transferria/transferria/pkg/abstract"
)

type ListableFilter interface {
	ListTables() ([]abstract.TableID, error)
}

type FilterableFilter interface {
	ListFilters() ([]abstract.TableDescription, error)
}
