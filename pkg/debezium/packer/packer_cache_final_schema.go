package packer

import (
	"sync"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
)

type PackerCacheFinalSchema struct {
	packer Packer
	mutex  sync.RWMutex
	cache  map[string][]byte // some string schema cache -> final schema
}

func (c *PackerCacheFinalSchema) GetSchemaIDResolver() SchemaIDResolver {
	return c.packer.GetSchemaIDResolver()
}

func (c *PackerCacheFinalSchema) Pack(
	changeItem *abstract.ChangeItem,
	payloadBuilder BuilderFunc,
	kafkaSchemaBuilder BuilderFunc,
	_ []byte,
) ([]byte, error) {
	finalSchema, err := c.BuildFinalSchema(changeItem, kafkaSchemaBuilder)
	if err != nil {
		return nil, xerrors.Errorf("unable to get schema via cache, err: %w", err)
	}
	return c.packer.Pack(
		changeItem,
		payloadBuilder,
		kafkaSchemaBuilder,
		finalSchema,
	)
}

func (c *PackerCacheFinalSchema) BuildFinalSchema(changeItem *abstract.ChangeItem, schemaBuilder BuilderFunc) ([]byte, error) {
	tableSchemaKeyV := tableSchemaKey(changeItem)

	c.mutex.RLock()
	result, ok := c.cache[tableSchemaKeyV]
	c.mutex.RUnlock()

	if ok {
		return result, nil
	} else {
		c.mutex.Lock()
		defer c.mutex.Unlock()

		// for the case, when cache was filled during waiting of lock
		if result, ok = c.cache[tableSchemaKeyV]; ok {
			return result, nil
		}

		finalSchema, err := c.packer.BuildFinalSchema(changeItem, schemaBuilder)
		if err != nil {
			return nil, xerrors.Errorf("unable to build schema, err: %w", err)
		}
		c.cache[tableSchemaKeyV] = finalSchema
		return finalSchema, nil
	}
}

func (c *PackerCacheFinalSchema) IsDropSchema() bool {
	return c.packer.IsDropSchema()
}

func NewPackerCacheFinalSchema(in Packer) *PackerCacheFinalSchema {
	return &PackerCacheFinalSchema{
		packer: in,
		mutex:  sync.RWMutex{},
		cache:  make(map[string][]byte),
	}
}
