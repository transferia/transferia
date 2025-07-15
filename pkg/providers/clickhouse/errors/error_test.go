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
