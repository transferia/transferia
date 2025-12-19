package mysql

import (
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
)

func init() {
	typesystem.AddFallbackSourceFactory(func() typesystem.Fallback {
		return typesystem.Fallback{
			To: 10,
			Picker: func(endpoint model.EndpointParams) bool {
				if endpoint.GetProviderType() != ProviderType {
					return false
				}

				srcParams, ok := endpoint.(*MysqlSource)
				if !ok {
					return false
				}

				return !srcParams.IsHomo
			},
			Function: func(item *abstract.ChangeItem) (*abstract.ChangeItem, error) {
				if !item.IsRowEvent() {
					return item, typesystem.FallbackDoesNotApplyErr
				}

				fallbackApplied := false
				for i := 0; i < len(item.TableSchema.Columns()); i++ {
					colSchema := item.TableSchema.Columns()[i]
					if colSchema.OriginalType == "mysql:datetime" {
						fallbackApplied = true

						columnIndex := item.ColumnNameIndex(colSchema.ColumnName)
						timeValue, ok := item.ColumnValues[columnIndex].(time.Time)
						if !ok {
							return nil, xerrors.Errorf("expected tipe time.Time in column %s", colSchema.ColumnName)
						}

						item.ColumnValues[columnIndex] = changeLocationToUTC(timeValue)
					}
				}

				if !fallbackApplied {
					return item, typesystem.FallbackDoesNotApplyErr
				}

				return item, nil
			},
		}
	})
}

func changeLocationToUTC(t time.Time) time.Time {
	year, month, day := t.Date()
	hour, minute, sec := t.Clock()
	nanoSec := t.Nanosecond()

	return time.Date(year, month, day, hour, minute, sec, nanoSec, time.UTC)
}
