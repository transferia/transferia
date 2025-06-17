//go:build !disable_mongo_provider

package mongo

import (
	"fmt"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// ValueInMongoFormat extracts the column value at the given index and converts it to MongoDB format
func ValueInMongoFormat(item *abstract.ChangeItem, index int) (interface{}, error) {
	if item == nil {
		return nil, abstract.NewFatalError(xerrors.New("impossible to extract value from a nil item"))
	}
	if index >= len(item.ColumnValues) {
		return nil, abstract.NewFatalError(xerrors.Errorf("ColumnValues[%d] does not exist, ColumnValues only has %d elements", index, len(item.ColumnValues)))
	}

	result, err := toMongoFormatUnguided(item.ColumnValues[index])
	if err != nil {
		return nil, xerrors.Errorf("failed to convert to MongoDB format: %v: %w", item.ColumnValues[index], err)
	}
	return result, nil
}

func toMongoFormatUnguided(v interface{}) (interface{}, error) {
	switch vC := v.(type) {
	case uint64:
		if result, err := primitive.ParseDecimal128(fmt.Sprintf("%d", vC)); err != nil {
			return nil, xerrors.Errorf("failed to convert %T to Decimal128: %w", vC, err)
		} else {
			return result, nil
		}
	default:
		return v, nil
	}
}
