package table

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract2"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

const (
	YtOriginalTypePropertyKey = abstract.PropertyKey("yt:originalType")
)

type YtColumn interface {
	abstract2.Column
	setTable(abstract2.Table)
	YtType() ytschema.ComplexType
}

type column struct {
	name       string
	ytType     ytschema.ComplexType
	ytCol      ytschema.Column
	typ        abstract2.Type
	tbl        abstract2.Table
	isOptional bool
}

func (c *column) Table() abstract2.Table {
	return c.tbl
}

func (c *column) Name() string {
	return c.name
}

func (c *column) FullName() string {
	return c.name
}

func (c *column) Type() abstract2.Type {
	return c.typ
}

func (c *column) YtType() ytschema.ComplexType {
	return c.ytType
}

func (c *column) Value(val interface{}) (abstract2.Value, error) {
	panic("not implemented")
}

func (c *column) Nullable() bool {
	return c.isOptional
}

func (c *column) Key() bool {
	return c.ytCol.SortOrder != ytschema.SortNone
}

func (c *column) ToOldColumn() (*abstract.ColSchema, error) {
	typ, err := c.Type().ToOldType()
	if err != nil {
		return nil, err
	}
	s := abstract.NewColSchema(c.Name(), typ, false)
	s.Required = !c.isOptional
	s.PrimaryKey = c.Key()

	if _, isPrimitive := c.ytType.(ytschema.Type); !isPrimitive {
		// It is much harder to restore nested original complex types by using s.OriginalType. Problem is that
		// c.ytType is schema.ComplexType interface what makes it unrecoverable just from json.Marshal(c.ytType),
		// we also need to store exact type of c.ytType and all nested types (e.g. schema.List).
		// So, ytType is stored as interface{} in Properties map.
		s.AddProperty(YtOriginalTypePropertyKey, c.ytType)
	}

	return &s, nil
}

func (c *column) setTable(t abstract2.Table) {
	c.tbl = t
}

// IsNullTyped reports whether the column has YT type "null". Such a column holds
// only null values and maps to the Skiff "nothing" wire type, which carries no
// payload, so it is excluded from the Skiff format and filled with nil instead.
func IsNullTyped(col YtColumn) bool {
	ytType, isPrimitive := col.YtType().(ytschema.Type)
	return isPrimitive && ytType == ytschema.TypeNull
}

func NewColumn(name string, typ abstract2.Type, ytType ytschema.ComplexType, ytCol ytschema.Column, isOptional bool) YtColumn {
	return &column{
		name:       name,
		ytType:     ytType,
		ytCol:      ytCol,
		typ:        typ,
		tbl:        nil,
		isOptional: isOptional,
	}
}
