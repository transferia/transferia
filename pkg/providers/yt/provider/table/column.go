package table

import (
	"github.com/transferia/transferia/pkg/abstract"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

const (
	YtOriginalTypePropertyKey = abstract.PropertyKey("yt:originalType")
)

// YtColumn is the passive-layer column abstraction. It intentionally does not
// embed any active-plane column interface; the passive layer speaks only in
// terms of YT schema types plus the small set of accessors below that
// downstream code (schema loader, skiff format builder, arena converter,
// row decoder) actually consumes.
type YtColumn interface {
	Name() string
	FullName() string
	Table() YtTable
	YtType() ytschema.ComplexType
	Nullable() bool
	Key() bool
	ToOldColumn() (*abstract.ColSchema, error)

	setTable(YtTable)
}

type column struct {
	name       string
	ytType     ytschema.ComplexType
	ytCol      ytschema.Column
	primType   ytschema.Type
	tbl        YtTable
	isOptional bool
}

var _ YtColumn = (*column)(nil)

func (c *column) Table() YtTable {
	return c.tbl
}

func (c *column) Name() string {
	return c.name
}

func (c *column) FullName() string {
	return c.name
}

func (c *column) YtType() ytschema.ComplexType {
	return c.ytType
}

func (c *column) Nullable() bool {
	return c.isOptional
}

func (c *column) Key() bool {
	return c.ytCol.SortOrder != ytschema.SortNone
}

func (c *column) ToOldColumn() (*abstract.ColSchema, error) {
	s := abstract.NewColSchema(c.Name(), c.primType, false)
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

func (c *column) setTable(t YtTable) {
	c.tbl = t
}

// IsNullTyped reports whether the column has YT type "null". Such a column holds
// only null values and maps to the Skiff "nothing" wire type, which carries no
// payload, so it is excluded from the Skiff format and filled with nil instead.
func IsNullTyped(col YtColumn) bool {
	ytType, isPrimitive := col.YtType().(ytschema.Type)
	return isPrimitive && ytType == ytschema.TypeNull
}

// NewColumn constructs a YtColumn. primType is the "flat" primitive type used
// when materializing the legacy abstract.ColSchema (composite YT types collapse
// to ytschema.TypeAny — see types.Resolve).
func NewColumn(name string, primType ytschema.Type, ytType ytschema.ComplexType, ytCol ytschema.Column, isOptional bool) YtColumn {
	return &column{
		name:       name,
		ytType:     ytType,
		ytCol:      ytCol,
		primType:   primType,
		tbl:        nil,
		isOptional: isOptional,
	}
}
