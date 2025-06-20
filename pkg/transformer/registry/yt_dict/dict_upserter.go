//go:build !disable_yt_provider

package ytdict

import (
	"encoding/json"
	"time"

	"github.com/transferia/transferia/pkg/providers/yt/provider/types"
	"go.ytsaurus.tech/yt/go/schema"
	"golang.org/x/xerrors"
)

func upsertToDict(dict, key, val any, keyComplexType schema.ComplexType) (any, error) {
	switch keySchema := keyComplexType.(type) {
	case schema.Type:
		key, err := types.CastPrimitiveToOldValue(key, keySchema)
		if err != nil {
			return nil, xerrors.Errorf("unable to cast primitive key of type '%T': %w", key, err)
		}
		return upsertPrimitiveToDict(dict, key, val, keySchema)

	case schema.Tagged:
		res, err := upsertToDict(dict, key, val, keySchema.Item)
		if err != nil {
			return nil, xerrors.Errorf("unable to process key schema.Tagged('%s'): %w", keySchema.Tag, err)
		}
		return res, nil

	case schema.Decimal:
		return nil, xerrors.New("for now, Decimal is not supported by Data Transfer")

	default: // schema.Optional, schema.List, schema.Struct, schema.Tuple, schema.Dict, schema.Variant:
		return nil, xerrors.Errorf("'%T' is not allowed as dict key", keySchema)
	}
}

func upsertPrimitive[T comparable](dict, key, val any) (map[T]any, error) {
	if dict == nil {
		dict = make(map[T]any)
	}
	castedDict, ok := dict.(map[T]any)
	if !ok {
		return nil, xerrors.Errorf("unable to cast dict to '%T', got '%T'", map[T]any{}, dict)
	}
	castedKey, ok := key.(T)
	if !ok {
		return nil, xerrors.Errorf("unable to cast key to '%T', got '%T'", castedKey, key)
	}
	castedDict[castedKey] = val
	return castedDict, nil
}

//nolint:descriptiveerrors
func upsertPrimitiveToDict(dict, key, val any, keySchema schema.Type) (any, error) {
	switch keySchema {
	case schema.TypeInt64:
		return upsertPrimitive[int64](dict, key, val)
	case schema.TypeInt32:
		return upsertPrimitive[int32](dict, key, val)
	case schema.TypeInt16:
		return upsertPrimitive[int16](dict, key, val)
	case schema.TypeInt8:
		return upsertPrimitive[int8](dict, key, val)
	case schema.TypeUint64:
		return upsertPrimitive[uint64](dict, key, val)
	case schema.TypeUint32:
		return upsertPrimitive[uint32](dict, key, val)
	case schema.TypeUint16:
		return upsertPrimitive[uint16](dict, key, val)
	case schema.TypeUint8:
		return upsertPrimitive[uint8](dict, key, val)
	case schema.TypeFloat32:
		return upsertPrimitive[float32](dict, key, val)
	case schema.TypeFloat64:
		return upsertPrimitive[json.Number](dict, key, val)
	case schema.TypeString:
		return upsertPrimitive[string](dict, key, val)
	case schema.TypeBoolean:
		return upsertPrimitive[bool](dict, key, val)
	case schema.TypeDate, schema.TypeDatetime, schema.TypeTimestamp:
		return upsertPrimitive[time.Time](dict, key, val)
	case schema.TypeInterval:
		return upsertPrimitive[time.Duration](dict, key, val)
	default: // schema.TypeAny, schema.TypeBytes:
		return nil, xerrors.Errorf("'%s' is not allowed as dict key", keySchema.String())
	}
}
