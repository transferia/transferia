package errors

import (
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

func TestIsFatalClickhouseError(t *testing.T) {
	irrelevant := xerrors.New("irrelevant")
	chError := &clickhouse.Exception{Code: 160}
	fatalChErr := &clickhouse.Exception{Code: 62}

	require.False(t, IsClickhouseError(irrelevant), "irrelevant errors are not clickhouse errors")
	require.False(t, IsClickhouseError(xerrors.Errorf("oh: %w", irrelevant)), "wrapped irrelevant errors are not clickhouse errors")
	require.True(t, IsClickhouseError(chError), "non-fatal clickhouse error is still clickhouse error")
	require.True(t, IsClickhouseError(xerrors.Errorf("oh: %w", chError)), "wrapped non-fatal clickhouse error is still clickhouse error")
	require.True(t, IsClickhouseError(fatalChErr), "fatal clickhouse error is still clickhouse error")
	require.True(t, IsClickhouseError(xerrors.Errorf("oh: %w", fatalChErr)), "wrapped fatal clickhouse error is still clickhouse error")

	require.False(t, IsFatalClickhouseError(irrelevant), "irrelevant errors are not clickhouse fatal errors")
	require.False(t, IsFatalClickhouseError(xerrors.Errorf("oh: %w", irrelevant)), "wrapped irrelevant errors are not clickhouse fatal errors")
	require.False(t, IsFatalClickhouseError(chError), "should be non-fatal")
	require.False(t, IsFatalClickhouseError(xerrors.Errorf("oh: %w", chError)), "wrapped  non-fatal should be non-fatal")
	require.True(t, IsFatalClickhouseError(fatalChErr), "should be fatal error")
	require.True(t, IsFatalClickhouseError(xerrors.Errorf("oh: %w", fatalChErr)), "wrapped fatal should be fatal")
}

func TestIsTooManyPartitionsError(t *testing.T) {
	// as returned by the native driver
	tooManyPartitions := &clickhouse.Exception{
		Code:    252,
		Message: "Too many partitions for single INSERT block (more than 100). The limit is controlled by 'max_partitions_per_insert_block' setting.",
	}
	// as produced by httpclient.ParseCHException from an HTTP response body
	tooManyPartitionsHTTP := &clickhouse.Exception{
		Code:    252,
		Name:    "TOO_MANY_PARTS",
		Message: "DB::Exception: Too many partitions for single INSERT block (more than 100). The limit is controlled by 'max_partitions_per_insert_block' setting.",
	}
	// the merge backlog error shares code 252 (TOO_MANY_PARTS) but is transient backpressure
	tooManyParts := &clickhouse.Exception{
		Code:    252,
		Message: "Too many parts (3000 with average size of 34.35 MiB) in table 'db.table'. Merges are processing significantly slower than inserts",
	}

	require.True(t, IsTooManyPartitionsError(tooManyPartitions))
	require.True(t, IsTooManyPartitionsError(xerrors.Errorf("failed to commit: %w", tooManyPartitions)), "must match through wrapping")
	require.True(t, IsTooManyPartitionsError(tooManyPartitionsHTTP))
	require.False(t, IsTooManyPartitionsError(tooManyParts), "merge backlog error must be retried as is, without lifting limits")
	require.False(t, IsTooManyPartitionsError(xerrors.New("Too many partitions for single INSERT block")), "non-clickhouse errors must not match")
}
