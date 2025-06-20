//go:build !disable_yt_provider

package table

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/base"
	"go.ytsaurus.tech/yt/go/schema"
)

const (
	YtOriginalTypePropertyKey = abstract.PropertyKey("yt:originalType")
)

type YtColumn interface {
	base.Column
	setTable(base.Table)
	YtType() schema.ComplexType
}

type column struct {
	name       string
	ytType     schema.ComplexType
	ytCol      schema.Column
	typ        base.Type
	tbl        base.Table
	isOptional bool
}

func (c *column) Table() base.Table {
	return c.tbl
}

func (c *column) Name() string {
	return c.name
}

func (c *column) FullName() string {
	return c.name
}

func (c *column) Type() base.Type {
	return c.typ
}

func (c *column) YtType() schema.ComplexType {
	return c.ytType
}

func (c *column) Value(val interface{}) (base.Value, error) {
	panic("not implemented")
}

func (c *column) Nullable() bool {
	return c.isOptional
}

func (c *column) Key() bool {
	return c.ytCol.SortOrder != schema.SortNone
}

func (c *column) ToOldColumn() (*abstract.ColSchema, error) {
	typ, err := c.Type().ToOldType()
	if err != nil {
		return nil, err
	}
	s := abstract.NewColSchema(c.Name(), typ, false)
	s.Required = !c.isOptional
	s.PrimaryKey = c.Key()

	if _, isPrimitive := c.ytType.(schema.Type); !isPrimitive {
		// It is much harder to restore nested original complex types by using s.OriginalType. Problem is that
		// c.ytType is schema.ComplexType interface what makes it unrecoverable just from json.Marshal(c.ytType),
		// we also need to store exact type of c.ytType and all nested types (e.g. schema.List).
		// So, ytType is stored as interface{} in Properties map.
		s.AddProperty(YtOriginalTypePropertyKey, c.ytType)
	}

	return &s, nil
}

func (c *column) setTable(t base.Table) {
	c.tbl = t
}

func NewColumn(name string, typ base.Type, ytType schema.ComplexType, ytCol schema.Column, isOptional bool) YtColumn {
	return &column{
		name:       name,
		ytType:     ytType,
		ytCol:      ytCol,
		typ:        typ,
		tbl:        nil,
		isOptional: isOptional,
	}
}
