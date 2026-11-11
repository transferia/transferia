package tests

import (
	"context"
	"database/sql"
	sql_driver "database/sql/driver"
	"io"
	"net"
	"regexp"
	"syscall"
	"testing"
	"time"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
)

func newReadRetryTestClient(test *testing.T) (*ch_eo_common.Client, sqlmock.Sqlmock) {
	test.Helper()
	database, mock, err := sqlmock.New()
	require.NoError(test, err)
	// A leaked result would prevent the next attempt from acquiring a connection.
	database.SetMaxOpenConns(1)
	test.Cleanup(
		func() {
			require.NoError(test, mock.ExpectationsWereMet())
			_ = database.Close()
		},
	)
	return newQueryTestClient(database), mock
}

func TestTableUUIDRetriesTransportError(test *testing.T) {
	for _, failureStage := range []string{"query", "scan"} {
		test.Run(failureStage, func(test *testing.T) {
			client, mock := newReadRetryTestClient(test)
			statement := regexp.QuoteMeta("SELECT toString(uuid) FROM system.tables WHERE database = ? AND name = ?")
			failedQuery := mock.ExpectQuery(statement).WithArgs("test", "events")
			if failureStage == "query" {
				failedQuery.WillReturnError(io.EOF)
			} else {
				// database/sql treats plain EOF as the end of a result; a
				// truncated row must surface as a non-EOF transport error.
				failedQuery.WillReturnRows(sqlmock.NewRows([]string{"uuid"}).AddRow("unused").RowError(0, io.ErrUnexpectedEOF)).RowsWillBeClosed()
			}
			expectedUUID := "1430a6e2-4a12-4200-936c-71276dc4e480"
			mock.ExpectQuery(statement).WithArgs("test", "events").WillReturnRows(sqlmock.NewRows([]string{"uuid"}).AddRow(expectedUUID)).RowsWillBeClosed()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			actualUUID, err := client.TableUUID(ctx, "unused", "events")
			require.NoError(test, err)
			require.Equal(test, expectedUUID, actualUUID)
		})
	}
}

func TestReadStringsDiscardsPartialResultBeforeRetry(test *testing.T) {
	client, mock := newReadRetryTestClient(test)
	statement := "SELECT partition_id FROM parts WHERE table = ?"
	truncatedRows := sqlmock.NewRows([]string{"partition_id"}).AddRow("202609").AddRow("unread").RowError(1, io.ErrUnexpectedEOF)
	mock.ExpectQuery(regexp.QuoteMeta(statement)).WithArgs("events").WillReturnRows(truncatedRows).RowsWillBeClosed()
	mock.ExpectQuery(regexp.QuoteMeta(statement)).WithArgs("events").WillReturnRows(sqlmock.NewRows([]string{"partition_id"}).AddRow("202609").AddRow("202610")).RowsWillBeClosed()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	partitions, err := client.ReadStrings(ctx, "unused", statement, "events")
	require.NoError(test, err)
	require.Equal(test, []string{"202609", "202610"}, partitions)
}

func TestReadStringsNeverReturnsPartialResultAfterFailedRetries(test *testing.T) {
	client, mock := newReadRetryTestClient(test)
	statement := "SELECT partition_id FROM parts"
	for buildAttemptNumber := 0; buildAttemptNumber < 3; buildAttemptNumber++ {
		truncatedRows := sqlmock.NewRows([]string{"partition_id"}).AddRow("202609").AddRow("unread").RowError(1, io.ErrUnexpectedEOF)
		mock.ExpectQuery(regexp.QuoteMeta(statement)).WillReturnRows(truncatedRows).RowsWillBeClosed()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	partitions, err := client.ReadStrings(ctx, "unused", statement)
	require.ErrorIs(test, err, io.ErrUnexpectedEOF)
	require.Nil(test, partitions)
}

func TestReadRowDoesNotRetryPermanentErrors(test *testing.T) {
	for _, failureKind := range []string{"authentication", "schema", "missing_row", "conversion"} {
		test.Run(failureKind, func(test *testing.T) {
			client, mock := newReadRetryTestClient(test)
			statement := "SELECT count() FROM events"
			expectedQuery := mock.ExpectQuery(regexp.QuoteMeta(statement))
			var expectedError error
			switch failureKind {
			case "authentication":
				expectedError = &clickhouse_go.Exception{Code: 516, Message: "Authentication failed"}
				expectedQuery.WillReturnError(expectedError)
			case "schema":
				expectedError = &clickhouse_go.Exception{Code: 60, Message: "Unknown table"}
				expectedQuery.WillReturnError(expectedError)
			case "missing_row":
				expectedError = sql.ErrNoRows
				expectedQuery.WillReturnRows(sqlmock.NewRows([]string{"count"})).RowsWillBeClosed()
			case "conversion":
				expectedQuery.WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow("not-a-number")).RowsWillBeClosed()
			}
			var rowCount uint64
			err := client.ReadRow(context.Background(), "unused", statement, nil, &rowCount)
			if expectedError != nil {
				require.ErrorIs(test, err, expectedError)
			} else {
				require.ErrorContains(test, err, "converting driver.Value")
			}
		})
	}
}

func TestReadRetryCancellationInterruptsBackoff(test *testing.T) {
	client, _ := newReadRetryTestClient(test)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queryCalls := 0
	err := client.RetrySafeQuery(
		ctx,
		"unused",
		func() error {
			queryCalls++
			// The first retry waits 100 ms. Cancel while that wait is pending.
			time.AfterFunc(20*time.Millisecond, cancel)
			return io.EOF
		},
	)
	require.ErrorIs(test, err, context.Canceled)
	require.Equal(test, 1, queryCalls)
}

func TestReadRetryRecognizesTransportErrors(test *testing.T) {
	for errorName, transportError := range map[string]error{
		"eof":                io.EOF,
		"truncated_response": io.ErrUnexpectedEOF,
		"bad_connection":     sql_driver.ErrBadConn,
		"closed_socket":      net.ErrClosed,
		"connection_pool":    clickhouse_go.ErrAcquireConnTimeout,
		"connection_reset":   syscall.ECONNRESET,
		"broken_pipe":        syscall.EPIPE,
		"connection_refused": syscall.ECONNREFUSED,
		"wrapped_network":    &net.OpError{Op: "read", Net: "tcp", Err: io.EOF},
		"server_socket_read": &clickhouse_go.Exception{Code: 95, Message: "Cannot read from socket"},
		"server_timeout":     &clickhouse_go.Exception{Code: 209, Message: "Socket timeout"},
		"server_network":     &clickhouse_go.Exception{Code: 210, Message: "Network error"},
		"server_connections": &clickhouse_go.Exception{Code: 279, Message: "All connection tries failed"},
	} {
		test.Run(errorName, func(test *testing.T) {
			require.True(test, ch_eo_common.IsRetryableReadError(xerrors.Errorf("query failed: %w", transportError)))
		})
	}
}

func TestReadRetryRejectsPermanentErrors(test *testing.T) {
	for errorName, permanentError := range map[string]error{
		"no_error":          nil,
		"canceled":          context.Canceled,
		"expired":           context.DeadlineExceeded,
		"driver_closed":     clickhouse_go.ErrConnectionClosed,
		"missing_row":       sql.ErrNoRows,
		"unknown_error":     xerrors.New("unknown error"),
		"explicitly_fatal":  abstract.NewFatalError(io.EOF),
		"server_auth":       &clickhouse_go.Exception{Code: 516, Message: "Authentication failed"},
		"server_readonly":   &clickhouse_go.Exception{Code: 164, Message: "Readonly setting"},
		"server_schema":     &clickhouse_go.Exception{Code: 60, Message: "Unknown table"},
		"unknown_setting":   &clickhouse_go.Exception{Code: 115, Message: "Unknown setting"},
		"corrupt_file":      &clickhouse_go.Exception{Code: 3, Message: "Unexpected end of file"},
		"corrupt_file_read": &clickhouse_go.Exception{Code: 32, Message: "Attempt to read after EOF"},
	} {
		test.Run(errorName, func(test *testing.T) {
			require.False(test, ch_eo_common.IsRetryableReadError(permanentError))
		})
	}
}

func newQueryTestClient(database *sql.DB) *ch_eo_common.Client {
	return ch_eo_common.NewClient(&clickhouse_model.ChDestination{Database: "test"}, logger.Log,
		[][]ch_eo_common.Node{{ch_eo_common.NewNode(&conn_clickhouse.Host{Name: "unused"}, database, nil)}})
}
