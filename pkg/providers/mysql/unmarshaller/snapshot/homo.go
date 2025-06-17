//go:build !disable_mysql_provider

package snapshot

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

func unmarshalHomo(value interface{}, colSchema *abstract.ColSchema) (any, error) {
	if value == nil {
		return nil, nil
	}

	var result any
	var err error

	if valueHomo, ok := value.(abstract.HomoValuer); ok {
		return valueHomo.HomoValue(), nil
	}

	result, err = unmarshalHetero(value, colSchema)
	if err != nil {
		return nil, xerrors.Errorf("hetero unmarshalling failed: %w", err)
	}
	return result, nil
}
