//go:build !disable_bigquery_provider

package bigquery

import (
	"encoding/json"
	"fmt"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/spf13/cast"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/yt/go/schema"
)

var _ bigquery.ValueSaver = (*ChangeItem)(nil)

type ChangeItem struct {
	abstract.ChangeItem
}

func (c ChangeItem) Save() (row map[string]bigquery.Value, insertID string, err error) {
	res := map[string]bigquery.Value{}
	for i, val := range c.ColumnValues {
		res[c.ColumnNames[i]], err = typeFit(
			c.TableSchema.FastColumns()[abstract.ColumnName(c.ColumnNames[i])],
			val,
		)
		if err != nil {
			return nil, "", xerrors.Errorf("unable to type-fit: %w", err)
		}
	}
	return res, fmt.Sprintf("%s/%v/%s", c.Table, c.LSN, c.TxID), nil
}

func typeFit(col abstract.ColSchema, val any) (bigquery.Value, error) {
	switch schema.Type(col.DataType) {
	case schema.TypeAny:
		jsonData, err := json.Marshal(val)
		if err != nil {
			return nil, xerrors.Errorf("unable to marshal data: %w", err)
		}
		return jsonData, nil
	case schema.TypeDate:
		return civil.DateOf(cast.ToTime(val)), nil
	case schema.TypeTimestamp:
		return val, nil
	case schema.TypeDatetime:
		return civil.DateTimeOf(cast.ToTime(val)), nil
	case schema.TypeUint8:
		return cast.ToInt8(val), nil
	case schema.TypeUint16:
		return cast.ToInt16(val), nil
	case schema.TypeUint32:
		return cast.ToInt32(val), nil
	case schema.TypeUint64:
		return cast.ToInt64(val), nil
	default:
		return val, nil
	}
}
