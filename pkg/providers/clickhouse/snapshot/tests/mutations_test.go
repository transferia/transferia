package tests

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

func TestWaitForMutations(test *testing.T) {
	for _, scenario := range []string{"already_done", "finishes_after_poll", "deadline_while_waiting", "deadline_during_query", "query_error"} {
		test.Run(scenario, func(test *testing.T) {
			database, mock, err := sqlmock.New()
			require.NoError(test, err)
			defer database.Close()

			client := newQueryTestClient(database)
			ctx := context.Background()
			if scenario == "deadline_while_waiting" || scenario == "deadline_during_query" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
			}
			query := mock.ExpectQuery("SELECT count\\(\\) FROM system.mutations").WithArgs("test", "events")
			queryError := xerrors.New("query failed")
			switch scenario {
			case "already_done":
				query.WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(uint64(0)))
			case "finishes_after_poll":
				query.WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(uint64(2)))
				mock.ExpectQuery("SELECT count\\(\\) FROM system.mutations").WithArgs("test", "events").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(uint64(0)))
			case "deadline_while_waiting":
				query.WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(uint64(2)))
			case "deadline_during_query":
				query.WillDelayFor(time.Second).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(uint64(0)))
			case "query_error":
				query.WillReturnError(queryError)
			}
			err = client.WaitForMutations(ctx, client.NodesByShard()[0][0], "events")
			switch scenario {
			case "already_done", "finishes_after_poll":
				require.NoError(test, err)
			case "deadline_while_waiting", "deadline_during_query":
				require.ErrorIs(test, err, context.DeadlineExceeded)
				require.ErrorContains(test, err, "events")
				require.ErrorContains(test, err, "unused")
			case "query_error":
				require.ErrorIs(test, err, queryError)
			}
			require.NoError(test, mock.ExpectationsWereMet())
		})
	}
}
