package lightningcache

import (
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/debezium/packer"
)

type SessionPackersSchemaIDCache struct {
	keyPacker *PackerLightningCache
	valPacker *PackerLightningCache
}

func (c *SessionPackersSchemaIDCache) Packer(isKey bool) packer.Packer {
	if isKey {
		return c.keyPacker
	} else {
		return c.valPacker
	}
}

func (c *SessionPackersSchemaIDCache) IsUseAnyCache() bool {
	return c.keyPacker.IsUseAnyCache() || c.valPacker.IsUseAnyCache()
}

func (c *SessionPackersSchemaIDCache) IsUseCache(isKey bool) bool {
	if isKey {
		if c.keyPacker == nil {
			return false
		}
		return c.keyPacker.IsUseAnyCache()
	} else {
		if c.valPacker == nil {
			return false
		}
		return c.valPacker.IsUseAnyCache()
	}
}

func (c *SessionPackersSchemaIDCache) IsUseSchemaID(isKey bool) bool {
	if isKey {
		return c.keyPacker.IsUseSchemaID()
	} else {
		return c.valPacker.IsUseSchemaID()
	}
}

func (c *SessionPackersSchemaIDCache) IsNewSchema(isKey bool, changeItem *abstract.ChangeItem) bool {
	if isKey {
		if c.keyPacker == nil {
			return false // bcs in this case we don't need to convert schema to confluent format & resolve it
		}
		return c.keyPacker.IsNewSchema(changeItem)
	} else {
		if c.valPacker == nil {
			return false // bcs in this case we don't need to convert schema to confluent format & resolve it
		}
		return c.valPacker.IsNewSchema(changeItem)
	}
}

func (c *SessionPackersSchemaIDCache) GetAndSaveFinalSchemaAndMaybeSchemaID(isKey bool, changeItem *abstract.ChangeItem, finalSchema []byte) error {
	if isKey {
		if c.keyPacker == nil {
			return xerrors.Errorf("key packer is not schema_registry")
		}
		err := c.keyPacker.GetAndSaveSchemaID(changeItem, finalSchema)
		if err != nil {
			return xerrors.Errorf("unable to get & save key schemaID, err: %w", err)
		}
		return nil
	} else {
		if c.valPacker == nil {
			return xerrors.Errorf("val packer is not schema_registry")
		}
		err := c.valPacker.GetAndSaveSchemaID(changeItem, finalSchema)
		if err != nil {
			return xerrors.Errorf("unable to get & save val schemaID, err: %w", err)
		}
		return nil
	}
}

func NewSessionPackersSchemaIDCache(keyPacker, valPacker packer.Packer) (*SessionPackersSchemaIDCache, error) {
	return &SessionPackersSchemaIDCache{
		keyPacker: NewPackerLightningCache(keyPacker),
		valPacker: NewPackerLightningCache(valPacker),
	}, nil
}
