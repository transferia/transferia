//go:build !disable_clickhouse_provider

package clickhouse

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDropPossibleTimeZoneInfo(t *testing.T) {
	dateVal := time.Date(2024, 7, 7, 7, 0, 0, 0, time.UTC)
	stringVal := "Some other value"

	sT := new(sinkTable)
	sT.timezone = time.UTC

	// for non time.Time interfaces it acts as a passthrough
	res := removePKTimezoneInfo(sT, []interface{}{stringVal})
	strVal, ok := res[0].(string)
	require.True(t, ok)
	require.Equal(t, stringVal, strVal)

	// UCT timezone info is dropped
	res = removePKTimezoneInfo(sT, []interface{}{dateVal})
	date, ok := res[0].(string)
	require.True(t, ok)
	require.Contains(t, dateVal.String(), "+0000 UTC")
	require.NotContains(t, date, "+0000 UTC")
	require.Equal(t, "2024-07-07 07:00:00", date)

	// Timezone is converted to the one specified by sT.timezone and timezone info is dropped
	newYorkTZ, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	sT.timezone = newYorkTZ
	res = removePKTimezoneInfo(sT, []interface{}{dateVal})
	dateNY, ok := res[0].(string)
	require.True(t, ok)
	require.Equal(t, "2024-07-07 03:00:00", dateNY)
}
