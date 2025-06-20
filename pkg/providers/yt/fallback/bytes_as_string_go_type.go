//go:build !disable_yt_provider

package fallback

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"github.com/transferia/transferia/pkg/providers/yt"
	"go.ytsaurus.tech/yt/go/schema"
)

func patchTableSchema(ci *abstract.ChangeItem) *abstract.TableSchema {
	patchedTableSchema := ci.TableSchema.Copy()

	for i := 0; i < len(ci.TableSchema.Columns()); i++ {
		schemaType := schema.Type(ci.TableSchema.Columns()[i].DataType)
		if schemaType == schema.TypeBytes {
			patchedTableSchema.Columns()[i].DataType = schema.TypeString.String()
		}
	}
	return patchedTableSchema
}

func getCachedPatchedSchema(ci *abstract.ChangeItem, cache map[string]*abstract.TableSchema) (schema *abstract.TableSchema, err error) {
	originalTableSchema := ci.TableSchema
	originalTableSchemaHash, err := originalTableSchema.Hash()
	if err != nil {
		return nil, xerrors.Errorf("cannot get schema hash: %w", err)
	}

	cachedPatchedTableSchema, ok := cache[originalTableSchemaHash]
	if !ok {
		patchedTableSchema := patchTableSchema(ci)
		cache[originalTableSchemaHash] = patchedTableSchema
		cachedPatchedTableSchema = patchedTableSchema
	}
	return cachedPatchedTableSchema, nil
}

func FallbackBytesAsStringGoType(ci *abstract.ChangeItem, cache map[string]*abstract.TableSchema) (*abstract.ChangeItem, error) {
	if !ci.IsRowEvent() {
		return ci, typesystem.FallbackDoesNotApplyErr
	}

	fallbackApplied := false
	cachedTableSchema, err := getCachedPatchedSchema(ci, cache)
	if err != nil {
		return nil, xerrors.Errorf("cannot get schema from cache: %w", err)
	}

	columnNamesToIndices := ci.ColumnNameIndices()
	for i := 0; i < len(ci.TableSchema.Columns()); i++ {
		schemaType := schema.Type(ci.TableSchema.Columns()[i].DataType)
		if schemaType == schema.TypeBytes {
			colName := ci.TableSchema.Columns()[i].ColumnName
			colIndex := columnNamesToIndices[colName]
			colValue := ci.ColumnValues[colIndex]
			if colValue == nil {
				fallbackApplied = true
			} else if colValueAsBytes, ok := colValue.([]byte); ok {
				ci.ColumnValues[colIndex] = string(colValueAsBytes)
				fallbackApplied = true
			} else {
				return nil, xerrors.Errorf("invalid value type for '%v' type in schema: expected '%T', actual '%T'",
					schemaType, colValueAsBytes, colValue)
			}
		}
	}
	if !fallbackApplied {
		return ci, typesystem.FallbackDoesNotApplyErr
	}
	ci.TableSchema = cachedTableSchema // fallback applied
	return ci, nil
}

func init() {
	typesystem.AddFallbackSourceFactory(func() typesystem.Fallback {
		tableSchemaCache := map[string]*abstract.TableSchema{}
		return typesystem.Fallback{
			To:     7,
			Picker: typesystem.ProviderType(yt.ProviderType),
			Function: func(ci *abstract.ChangeItem) (*abstract.ChangeItem, error) {
				return FallbackBytesAsStringGoType(ci, tableSchemaCache)
			},
		}
	})
}
