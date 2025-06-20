//go:build !disable_clickhouse_provider

package clickhouse

import (
	"database/sql"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/connection/clickhouse"
)

func TestMarshalField(t *testing.T) {
	var (
		i32               = int32(32)
		sqlNullInt32      = &sql.NullInt32{Int32: i32, Valid: true}
		sqlNullInt32Empty = &sql.NullInt32{Int32: 0, Valid: false}

		i64               = int64(64)
		sqlNullInt64      = &sql.NullInt64{Int64: i64, Valid: true}
		sqlNullInt64Empty = &sql.NullInt64{Int64: 0, Valid: false}

		sqlNullBool      = &sql.NullBool{Bool: true, Valid: true}
		sqlNullBoolEmpty = &sql.NullBool{Bool: false, Valid: false}

		f64                 = float64(64.0)
		sqlNullFloat64      = &sql.NullFloat64{Float64: f64, Valid: true}
		sqlNullFloat64Empty = &sql.NullFloat64{Float64: 0, Valid: false}

		sqlNullString      = &sql.NullString{String: "string", Valid: true}
		sqlNullStringEmpty = &sql.NullString{String: "", Valid: false}

		tTime, _         = time.Parse(time.RFC3339, "2021-11-08T00:00:00Z")
		sqlNullTime      = &sql.NullTime{Time: tTime, Valid: true}
		sqlNullTimeEmpty = &sql.NullTime{Time: time.Time{}, Valid: false}

		emptyBytes       []byte
		str              = "RawBytes"
		someBytes        = []byte(str)
		sqlRawBytes      = &someBytes
		sqlRawBytesEmpty = &emptyBytes
	)

	v, size := marshalField(sqlNullInt32, "")
	require.Equal(t, v, i32)
	require.Equal(t, size, uint64(unsafe.Sizeof(i32)))

	v, size = marshalField(sqlNullInt32Empty, "")
	require.Equal(t, v, nil)
	require.Equal(t, size, uint64(0))

	v, size = marshalField(sqlNullInt64, "")
	require.Equal(t, v, i64)
	require.Equal(t, size, uint64(unsafe.Sizeof(i64)))

	v, size = marshalField(sqlNullInt64Empty, "")
	require.Equal(t, v, nil)
	require.Equal(t, size, uint64(0))

	v, size = marshalField(sqlNullBool, "")
	require.Equal(t, v, true)
	require.Equal(t, size, uint64(unsafe.Sizeof(true)))

	v, size = marshalField(sqlNullBoolEmpty, "")
	require.Equal(t, v, nil)
	require.Equal(t, size, uint64(0))

	v, size = marshalField(sqlNullFloat64, "")
	require.Equal(t, v, 64.0)
	require.Equal(t, size, uint64(unsafe.Sizeof(f64)))

	v, size = marshalField(sqlNullFloat64Empty, "")
	require.Equal(t, v, nil)
	require.Equal(t, size, uint64(0))

	v, size = marshalField(sqlNullString, "")
	require.Equal(t, v, "string")
	require.Equal(t, size, uint64(len("string")))

	v, size = marshalField(sqlNullStringEmpty, "")
	require.Equal(t, v, nil)
	require.Equal(t, size, uint64(0))

	v, size = marshalField(sqlNullTime, "")
	require.Equal(t, v, tTime)
	require.Equal(t, size, uint64(unsafe.Sizeof(tTime)))

	v, size = marshalField(sqlNullTimeEmpty, "")
	require.Equal(t, v, nil)
	require.Equal(t, size, uint64(0))

	v, size = marshalField(sqlRawBytes, "")
	require.Equal(t, v, string(someBytes))
	require.Equal(t, size, uint64(len(someBytes)))

	v, size = marshalField(sqlRawBytesEmpty, "")
	require.Equal(t, v, nil)
	require.Equal(t, size, uint64(0))

	v, size = marshalField(&tTime, "")
	require.Equal(t, v, tTime)
	require.Equal(t, size, uint64(unsafe.Sizeof(tTime)))

	v, size = marshalField(someBytes, "Array(UInt8)")
	require.Equal(t, v, someBytes)
	require.Equal(t, size, uint64(len(someBytes)))

	v, size = marshalField(someBytes, "")
	require.Equal(t, v, string(someBytes))
	require.Equal(t, size, uint64(len(someBytes)))

	addrStr := &str
	v, size = marshalField(&addrStr, "")
	require.Equal(t, v, str)
	require.Equal(t, size, uint64(len(str)))

	strSlice := []string{"123", "456789"}
	v, size = marshalField(strSlice, "")
	require.Equal(t, v, strSlice)
	require.Equal(t, size, uint64(24+9))

	v, size = marshalField(&strSlice, "")
	require.Equal(t, v, strSlice)
	require.Equal(t, size, uint64(24+9))

	intSlice := []int64{0, 1}
	v, size = marshalField(intSlice, "")
	require.Equal(t, v, intSlice)
	require.Equal(t, size, uint64(24+16))

	duration, _ := time.ParseDuration("10h")
	v, size = marshalField(&duration, "")
	require.Equal(t, v, duration)
	require.Equal(t, size, uint64(unsafe.Sizeof(duration)))

	v, size = marshalField(duration, "")
	require.Equal(t, v, duration)
	require.Equal(t, size, uint64(unsafe.Sizeof(duration)))
}

func TestShardsToString(t *testing.T) {
	shards := map[string][]*clickhouse.Host{
		"shard1": {
			{Name: "host1", NativePort: 9000, HTTPPort: 8123, ShardName: "shard1"},
			{Name: "host2", NativePort: 9001, HTTPPort: 8124, ShardName: "shard1"},
		},
		"shard2": {
			{Name: "host3", NativePort: 9002, HTTPPort: 8125, ShardName: "shard2"},
		},
	}
	require.Equal(t, "map[shard1:[host1:[9000|8123] host2:[9001|8124]] shard2:[host3:[9002|8125]]]", ShardsToString(shards))
}

func TestHostsToString(t *testing.T) {
	hosts := []*clickhouse.Host{
		{Name: "host1", NativePort: 9000, HTTPPort: 8123, ShardName: "shard1"},
		{Name: "host2", NativePort: 9001, HTTPPort: 8124, ShardName: "shard1"},
	}
	require.Equal(t, "[host1:[9000|8123] host2:[9001|8124]]", HostsToString(hosts))
}
