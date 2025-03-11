package base

import (
	"github.com/transferia/transferia/pkg/abstract"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type DataObjectFilter interface {
	Includes(obj DataObject) (bool, error)
	IncludesID(tID abstract.TableID) (bool, error)
}

type DataObjects interface {
	// Iterator
	Next() bool
	Err() error
	Close()
	Object() (DataObject, error)

	// Support legacy
	ToOldTableMap() (abstract.TableMap, error)
}

type DataObject interface {
	Name() string
	FullName() string

	// Iterator
	Next() bool
	Err() error
	Close()
	Part() (DataObjectPart, error)

	// Support legacy
	ToOldTableID() (*abstract.TableID, error)
}

type DataObjectPart interface {
	Name() string
	FullName() string

	// Support legacy
	ToOldTableDescription() (*abstract.TableDescription, error)

	// Support new snapshot
	ToTablePart() (*abstract.TableDescription, error)
}

type Table interface {
	Database() string
	Schema() string
	Name() string
	FullName() string
	// TODO: Equals(otherTable Table) bool ?
	ColumnsCount() int
	Column(i int) Column
	ColumnByName(name string) Column

	// Support legacy
	ToOldTable() (*abstract.TableSchema, error)
}

type Column interface {
	Table() Table
	Name() string
	FullName() string
	// TODO: Equals(otherColumn Column) bool ?
	Type() Type
	Value(val interface{}) (Value, error)
	Nullable() bool
	Key() bool

	// Support legacy
	ToOldColumn() (*abstract.ColSchema, error)
}

type Value interface {
	Column() Column
	// TODO: Equals(otherValue Value) bool ?
	Value() interface{}
	// TODO: ToString() string

	// Support legacy
	ToOldValue() (interface{}, error)
}

// TODO: this interface should be sealed.
type Type interface {
	// TODO: Equals(otherType Type) bool ?
	Validate(value Value) error

	// Support legacy
	ToOldType() (yt_schema.Type, error)
}
