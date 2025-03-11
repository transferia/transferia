package tablequery

import "github.com/transferria/transferria/pkg/abstract"

type TableQuery struct {
	TableID abstract.TableID

	SortByPKeys bool

	Filter abstract.WhereStatement
	Offset uint64 // offset (in rows) along the ordering key (not necessary primary key)
	Limit  uint64
}

func NewTableQuery(
	tableID abstract.TableID,
	sortByPKeys bool,
	filter abstract.WhereStatement,
	offset uint64,
	limit uint64,
) *TableQuery {
	return &TableQuery{
		TableID:     tableID,
		SortByPKeys: sortByPKeys,
		Filter:      filter,
		Offset:      offset,
		Limit:       limit,
	}
}
