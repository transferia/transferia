package errors

import (
	stderrors "errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
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

func TestIsRetryableWithoutReset(t *testing.T) {
	mergeBacklog := "Too many parts. Merges are processing significantly slower than inserts."
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "native query limit", err: &clickhouse.Exception{Code: 202}, want: true},
		{name: "wrapped merge backlog", err: xerrors.Errorf("insert failed: %w", &clickhouse.Exception{Code: 252, Message: mergeBacklog}), want: true},
		{name: "HTTP merge backlog", err: httpclient.ParseCHException("Code: 252. DB::Exception: " + mergeBacklog + " (TOO_MANY_PARTS)"), want: true},
		{name: "partition limit", err: &clickhouse.Exception{Code: 252, Message: "Too many partitions for single INSERT block"}},
		{name: "other parts error", err: &clickhouse.Exception{Code: 252, Message: "Too many parts"}},
		{name: "unknown code with backlog text", err: &clickhouse.Exception{Code: 999, Message: mergeBacklog}},
		{name: "memory limit", err: &clickhouse.Exception{Code: 241}},
		{name: "query timeout", err: &clickhouse.Exception{Code: 159}},
		{name: "socket timeout", err: &clickhouse.Exception{Code: 209}},
		{name: "network failure", err: &clickhouse.Exception{Code: 210}},
		{name: "table not initialized", err: &clickhouse.Exception{Code: 667}},
		{name: "plain text", err: xerrors.New("Code: 252. DB::Exception: " + mergeBacklog)},
		{name: "mixed replica errors", err: xerrors.Errorf("DDL failed: %w", stderrors.Join(&clickhouse.Exception{Code: 202}, &clickhouse.Exception{Code: 667}))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, IsRetryableWithoutReset(tc.err))
		})
	}
}
