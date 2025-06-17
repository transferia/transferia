//go:build !disable_clickhouse_provider

package clickhouse

import (
	"fmt"
)

func buildQuery(selectQuery string, partRows int64, stateCurrent uint64, ioFormat string) string {
	query := selectQuery
	if partRows > 0 {
		query = fmt.Sprintf("%s LIMIT %v", query, partRows)
	}
	if stateCurrent > 0 {
		query = fmt.Sprintf("%s OFFSET %v", query, stateCurrent)
	}
	query = fmt.Sprintf("%s SETTINGS timeout_before_checking_execution_speed=0, format_csv_delimiter = ',' FORMAT %s", query, ioFormat)
	return query
}
