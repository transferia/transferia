//go:build !disable_clickhouse_provider

package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
)

func TestBuildQuery(t *testing.T) {
	require.Equal(t,
		`SELECT a,b FROM my_table SETTINGS timeout_before_checking_execution_speed=0, format_csv_delimiter = ',' FORMAT CSV`,
		buildQuery(`SELECT a,b FROM my_table`, 0, 0, string(model.ClickhouseIOFormatCSV)),
	)

	require.Equal(t,
		`SELECT a,b FROM my_table LIMIT 1 SETTINGS timeout_before_checking_execution_speed=0, format_csv_delimiter = ',' FORMAT CSV`,
		buildQuery(`SELECT a,b FROM my_table`, 1, 0, string(model.ClickhouseIOFormatCSV)),
	)

	require.Equal(t,
		`SELECT a,b FROM my_table OFFSET 1 SETTINGS timeout_before_checking_execution_speed=0, format_csv_delimiter = ',' FORMAT CSV`,
		buildQuery(`SELECT a,b FROM my_table`, 0, 1, string(model.ClickhouseIOFormatCSV)),
	)

	require.Equal(t,
		`SELECT a,b FROM my_table LIMIT 1 OFFSET 2 SETTINGS timeout_before_checking_execution_speed=0, format_csv_delimiter = ',' FORMAT JSONCompactEachRow`,
		buildQuery(`SELECT a,b FROM my_table`, 1, 2, string(model.ClickhouseIOFormatJSONCompact)),
	)
}
