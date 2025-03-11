package lightningcache

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/debezium"
	"github.com/transferia/transferia/pkg/debezium/packer"
)

func handleChangeItems(input []abstract.ChangeItem, schemaIDCache *SessionPackersSchemaIDCache, emitter *debezium.Emitter, isSnapshot bool) error {
	var err error
	for i := range input {
		if !input[i].IsRowEvent() {
			continue
		}
		if schemaIDCache.IsUseCache(true) {
			if schemaIDCache.IsNewSchema(true, &input[i]) {
				var finalSchema []byte = nil
				if schemaIDCache.IsUseSchemaID(true) {
					finalSchema, err = emitter.ToConfluentSchemaKey(&input[i], isSnapshot)
					if err != nil {
						return xerrors.Errorf("unable to build key schema, err: %w", err)
					}
				} else if !schemaIDCache.Packer(true).IsDropSchema() {
					finalSchema, err = emitter.ToKafkaSchemaKey(&input[i], isSnapshot)
					if err != nil {
						return xerrors.Errorf("unable to build key schema, err: %w", err)
					}
				}
				err = schemaIDCache.GetAndSaveFinalSchemaAndMaybeSchemaID(true, &input[i], finalSchema)
				if err != nil {
					return xerrors.Errorf("unable to get key schemaID and store it, err: %w", err)
				}
			}
		}
		if schemaIDCache.IsUseCache(false) {
			if schemaIDCache.IsNewSchema(false, &input[i]) {
				var finalSchema []byte = nil
				if schemaIDCache.IsUseSchemaID(false) {
					finalSchema, err = emitter.ToConfluentSchemaVal(&input[i], isSnapshot)
					if err != nil {
						return xerrors.Errorf("unable to build val schema, err: %w", err)
					}
				} else if !schemaIDCache.Packer(false).IsDropSchema() {
					finalSchema, err = emitter.ToKafkaSchemaVal(&input[i], isSnapshot)
					if err != nil {
						return xerrors.Errorf("unable to build val schema, err: %w", err)
					}
				}
				err = schemaIDCache.GetAndSaveFinalSchemaAndMaybeSchemaID(false, &input[i], finalSchema)
				if err != nil {
					return xerrors.Errorf("unable to get val schemaID and store it, err: %w", err)
				}
			}
		}
	}
	return nil
}

// NewLightningCache
// after NewLightningCache there are all schemaID/finalSchemas should be known
func NewLightningCache(emitter *debezium.Emitter, input []abstract.ChangeItem, isSnapshot bool) (packer.SessionPackers, error) {
	schemaIDCache, err := NewSessionPackersSchemaIDCache(emitter.GetPackers())
	if err != nil {
		return nil, xerrors.Errorf("unable to create packer cache, err: %w", err)
	}
	if !schemaIDCache.IsUseAnyCache() {
		return nil, nil
	}
	err = handleChangeItems(input, schemaIDCache, emitter, isSnapshot)
	if err != nil {
		return nil, xerrors.Errorf("unable to handle changeItems by parser cache, err: %w", err)
	}
	return schemaIDCache, nil
}
