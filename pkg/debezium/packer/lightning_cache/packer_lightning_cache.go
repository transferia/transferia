package lightningcache

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/debezium/packer"
)

type PackerLightningCache struct {
	originalPacker         packer.Packer
	isUseSchemaRegistry    bool
	schemaIDCacheByAddr    map[*abstract.ColSchema]uint32
	finalSchemaCacheByAddr map[*abstract.ColSchema][]byte
}

func schemaUniqueID(changeItem *abstract.ChangeItem) *abstract.ColSchema {
	return &changeItem.TableSchema.Columns()[0]
}

func (c *PackerLightningCache) IsUseSchemaID() bool {
	return c.isUseSchemaRegistry
}

func (c *PackerLightningCache) GetSchemaIDResolver() packer.SchemaIDResolver {
	return c
}

func (c *PackerLightningCache) PackWithSchemaID(schemaID uint32, payload []byte) ([]byte, error) {
	return c.originalPacker.GetSchemaIDResolver().PackWithSchemaID(schemaID, payload)
}

func (c *PackerLightningCache) ResolveSchemaID(schema []byte, table abstract.TableID) (uint32, error) {
	return c.originalPacker.GetSchemaIDResolver().ResolveSchemaID(schema, table)
}

func (c *PackerLightningCache) IsUseAnyCache() bool {
	return !c.originalPacker.IsDropSchema()
}

func (c *PackerLightningCache) IsNewSchema(changeItem *abstract.ChangeItem) bool {
	if c.isUseSchemaRegistry {
		_, ok := c.schemaIDCacheByAddr[schemaUniqueID(changeItem)]
		return !ok
	} else {
		_, ok := c.finalSchemaCacheByAddr[schemaUniqueID(changeItem)]
		return !ok
	}
}

func (c *PackerLightningCache) GetAndSaveSchemaID(changeItem *abstract.ChangeItem, finalSchema []byte) error {
	if c.isUseSchemaRegistry {
		schemaID, err := c.originalPacker.GetSchemaIDResolver().ResolveSchemaID(finalSchema, changeItem.TableID())
		if err != nil {
			return xerrors.Errorf("unable to get schemaID, err: %w", err)
		}
		c.schemaIDCacheByAddr[schemaUniqueID(changeItem)] = schemaID
	} else {
		c.finalSchemaCacheByAddr[schemaUniqueID(changeItem)] = finalSchema
	}
	return nil
}

func (c *PackerLightningCache) resolveSchemaIDFromCache(changeItem *abstract.ChangeItem) (uint32, error) {
	result, ok := c.schemaIDCacheByAddr[schemaUniqueID(changeItem)]
	if !ok {
		return 0, xerrors.Errorf("unknown item")
	}
	return result, nil
}

func (c *PackerLightningCache) Pack(
	changeItem *abstract.ChangeItem,
	payloadBuilder packer.BuilderFunc,
	schemaBuilder packer.BuilderFunc,
	maybeCachedRawSchema []byte,
) ([]byte, error) {
	if c.isUseSchemaRegistry {
		schemaID, err := c.resolveSchemaIDFromCache(changeItem)
		if err != nil {
			return nil, xerrors.Errorf("unable to resolve schema, err: %w", err)
		}
		rawPayload, err := payloadBuilder(changeItem)
		if err != nil {
			return nil, xerrors.Errorf("unable to build rawPayload, err: %w", err)
		}
		result, err := c.PackWithSchemaID(schemaID, rawPayload)
		if err != nil {
			return nil, xerrors.Errorf("unable to pack with schemaID, err: %w", err)
		}
		return result, nil
	} else {
		result, err := c.originalPacker.Pack(changeItem, payloadBuilder, schemaBuilder, maybeCachedRawSchema)
		if err != nil {
			return nil, xerrors.Errorf("unable to pack, err: %w", err)
		}
		return result, nil
	}
}

func (c *PackerLightningCache) BuildFinalSchema(_ *abstract.ChangeItem, _ packer.BuilderFunc) ([]byte, error) {
	return nil, xerrors.Errorf("PackerLightningCache::BuildFinalSchema unimplemented, never should be called")
}

func (c *PackerLightningCache) IsDropSchema() bool {
	return false
}

func NewPackerLightningCache(currPacker packer.Packer) *PackerLightningCache {
	return &PackerLightningCache{
		originalPacker:         currPacker,
		isUseSchemaRegistry:    currPacker.GetSchemaIDResolver() != nil,
		schemaIDCacheByAddr:    make(map[*abstract.ColSchema]uint32),
		finalSchemaCacheByAddr: make(map[*abstract.ColSchema][]byte),
	}
}
