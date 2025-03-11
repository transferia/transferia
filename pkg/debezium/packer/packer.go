package packer

import (
	"github.com/transferia/transferia/pkg/abstract"
)

type BuilderFunc = func(changeItem *abstract.ChangeItem) ([]byte, error)

// SchemaIDResolver
// If there are packer with schemaRegistry and wrapped into schemaID resolver
// It would be called ResolveSchemaID+PackWithSchemaID except 'Pack'
type SchemaIDResolver interface {
	ResolveSchemaID(schema []byte, table abstract.TableID) (uint32, error)
	PackWithSchemaID(schemaID uint32, payload []byte) ([]byte, error)
}

type Packer interface {
	// Pack  - builds payload & schema into message
	Pack(
		changeItem *abstract.ChangeItem,
		payloadBuilder BuilderFunc,
		kafkaSchemaBuilder BuilderFunc,
		maybeCachedRawSchema []byte,
	) ([]byte, error)

	// BuildFinalSchema
	// This interface is used for caching
	// If your packer is wrapped into 'PackerCacheFinalSchema' - it will call BuildFinalSchema only when schema is not cached
	BuildFinalSchema(changeItem *abstract.ChangeItem, kafkaSchemaBuilder BuilderFunc) ([]byte, error)
	IsDropSchema() bool

	GetSchemaIDResolver() SchemaIDResolver
}
