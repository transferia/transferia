//go:build !disable_postgres_provider

package sqltimestamp

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	parsed, err := Parse("10000-01-01 02:59:59+03")
	require.NoError(t, err)
	require.Equal(t, time.Date(10000, time.Month(0o1), 0o1, 2, 59, 59, 0, time.FixedZone("", 3*60*60)), parsed)

	parsed, err = Parse("2000-10-19")
	require.NoError(t, err)
	require.Equal(t, time.Date(2000, time.Month(10), 19, 0, 0, 0, 0, time.UTC), parsed)

	parsed, err = Parse("2000-10-19 BC")
	require.NoError(t, err)
	require.Equal(t, time.Date(-2000, time.Month(10), 19, 0, 0, 0, 0, time.UTC), parsed)

	parsed, err = Parse("2000-10-19 10:23:54.123")
	require.NoError(t, err)
	require.Equal(t, time.Date(2000, time.Month(10), 19, 10, 23, 54, 123000000, time.UTC), parsed)

	parsed, err = Parse("2000-10-19 10:23:54.123 BC")
	require.NoError(t, err)
	require.Equal(t, time.Date(-2000, time.Month(10), 19, 10, 23, 54, 123000000, time.UTC), parsed)

	parsed, err = Parse("2000-10-19 10:23:54.123456")
	require.NoError(t, err)
	require.Equal(t, time.Date(2000, time.Month(10), 19, 10, 23, 54, 123456000, time.UTC), parsed)

	parsed, err = Parse("2000-10-19 10:23:54.123456 BC")
	require.NoError(t, err)
	require.Equal(t, time.Date(-2000, time.Month(10), 19, 10, 23, 54, 123456000, time.UTC), parsed)

	parsed, err = Parse("2000-10-19 10:23:54.123456+01")
	require.NoError(t, err)
	require.Equal(t, time.Date(2000, time.Month(10), 19, 10, 23, 54, 123456000, time.FixedZone("", 1*60*60)), parsed)

	parsed, err = Parse("2000-10-19 10:23:54.123456+01 BC")
	require.NoError(t, err)
	require.Equal(t, time.Date(-2000, time.Month(10), 19, 10, 23, 54, 123456000, time.FixedZone("", 1*60*60)), parsed)

	parsed, err = Parse("2000-10-19 10:23:54.123456+01:02")
	require.NoError(t, err)
	require.Equal(t, time.Date(2000, time.Month(10), 19, 10, 23, 54, 123456000, time.FixedZone("", 1*60*60+2*60)), parsed)

	parsed, err = Parse("2000-10-19 10:23:54.123456+01:02 BC")
	require.NoError(t, err)
	require.Equal(t, time.Date(-2000, time.Month(10), 19, 10, 23, 54, 123456000, time.FixedZone("", 1*60*60+2*60)), parsed)

	parsed, err = Parse("2000-10-19 10:23:54.123456+01:02:03")
	require.NoError(t, err)
	require.Equal(t, time.Date(2000, time.Month(10), 19, 10, 23, 54, 123456000, time.FixedZone("", 1*60*60+2*60+3)), parsed)

	parsed, err = Parse("2000-10-19 10:23:54.123456+01:02:03 BC")
	require.NoError(t, err)
	require.Equal(t, time.Date(-2000, time.Month(10), 19, 10, 23, 54, 123456000, time.FixedZone("", 1*60*60+2*60+3)), parsed)

	parsed, err = Parse("40000-10-19")
	require.NoError(t, err)
	require.Equal(t, time.Date(40000, time.Month(10), 19, 0, 0, 0, 0, time.UTC), parsed)

	parsed, err = Parse("40000-10-19 10:23:54.123")
	require.NoError(t, err)
	require.Equal(t, time.Date(40000, time.Month(10), 19, 10, 23, 54, 123000000, time.UTC), parsed)

	parsed, err = Parse("40000-10-19 10:23:54.123456")
	require.NoError(t, err)
	require.Equal(t, time.Date(40000, time.Month(10), 19, 10, 23, 54, 123456000, time.UTC), parsed)

	parsed, err = Parse("40000-10-19 10:23:54.123456+01")
	require.NoError(t, err)
	require.Equal(t, time.Date(40000, time.Month(10), 19, 10, 23, 54, 123456000, time.FixedZone("", 1*60*60)), parsed)

	parsed, err = Parse("40000-10-19 10:23:54.123456+01:02")
	require.NoError(t, err)
	require.Equal(t, time.Date(40000, time.Month(10), 19, 10, 23, 54, 123456000, time.FixedZone("", 1*60*60+2*60)), parsed)

	parsed, err = Parse("40000-10-19 10:23:54.123456+01:02:03")
	require.NoError(t, err)
	require.Equal(t, time.Date(40000, time.Month(10), 19, 10, 23, 54, 123456000, time.FixedZone("", 1*60*60+2*60+3)), parsed)
}
