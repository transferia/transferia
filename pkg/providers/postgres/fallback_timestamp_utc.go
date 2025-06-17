//go:build !disable_postgres_provider

package postgres

import (
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgtype"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"go.ytsaurus.tech/library/go/core/log"
)

// FallbackTimestampToUTC implements https://st.yandex-team.ru/TM-5092
func FallbackTimestampToUTC(item *abstract.ChangeItem) (*abstract.ChangeItem, error) {
	if !item.IsRowEvent() {
		return item, typesystem.FallbackDoesNotApplyErr
	}

	fallbackApplied := false

	for i := 0; i < len(item.TableSchema.Columns()); i++ {
		if !IsPgTypeTimestampWithoutTimeZoneUnprefixed(strings.TrimPrefix(item.TableSchema.Columns()[i].OriginalType, "pg:")) {
			continue
		}

		cName := item.TableSchema.Columns()[i].ColumnName
		cIndex := item.ColumnNameIndex(cName)
		if cIndex < 0 {
			continue
		}
		cValueUncasted := item.ColumnValues[cIndex]
		if cValueUncasted == nil {
			continue
		}

		cValue, ok := extractTimeFromColumnValue(cValueUncasted)
		if !ok {
			logger.Log.Warn("failed to cast timestamp column to time.Time", log.String("column", cName), log.String("actual_type", fmt.Sprintf("%T", cValueUncasted)))
			continue
		}
		item.ColumnValues[cIndex] = time.Date(actualYear(cValue), cValue.Month(), cValue.Day(), cValue.Hour(), cValue.Minute(), cValue.Second(), cValue.Nanosecond(), time.UTC)
		fallbackApplied = true
	}

	if !fallbackApplied {
		return item, typesystem.FallbackDoesNotApplyErr
	}
	return item, nil
}

func extractTimeFromColumnValue(columnValue interface{}) (time.Time, bool) {
	// TODO(@kry127) can be removed after TM-5041 strict validation
	pgTimestamp, ok := columnValue.(pgtype.Timestamp)
	if ok {
		return pgTimestamp.Time, true
	}
	columnValueAsTime, ok := columnValue.(time.Time)
	if ok {
		return columnValueAsTime, true
	}
	return time.Time{}, false
}

func init() {
	typesystem.AddFallbackSourceFactory(func() typesystem.Fallback {
		return typesystem.Fallback{
			To:       3,
			Picker:   typesystem.ProviderType(ProviderType),
			Function: FallbackTimestampToUTC,
		}
	})
}
