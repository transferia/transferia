package ch_eo_common

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/model"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"go.ytsaurus.tech/library/go/core/log"
)

type clientFixture struct {
	tablesClient *Client
	dstConfig    *clickhouse_model.ChDestination
	logger       log.Logger
}

func newClientFixture(test *testing.T) *clientFixture {
	test.Helper()
	cfg := &clickhouse_model.ChDestination{Database: "test", Cleanup: model.Drop}
	client := NewClient(cfg, logger.Log, [][]Node{{NewNode(&conn_clickhouse.Host{Name: "unused"}, new(sql.DB), newSessionHTTPClient(func(url.Values) error { return nil }))}})
	return &clientFixture{tablesClient: client, dstConfig: cfg, logger: logger.Log}
}
func configureTestNodes(fixture *clientFixture, configure func([][]Node) [][]Node) {
	fixture.tablesClient = NewClient(fixture.dstConfig, fixture.logger, configure(fixture.tablesClient.NodesByShard()))
}

type sessionHTTPClient struct {
	httpclient.HTTPClient
	query func(url.Values) error
}

func newSessionHTTPClient(query func(url.Values) error) *sessionHTTPClient {
	return &sessionHTTPClient{HTTPClient: nil, query: query}
}

func (c *sessionHTTPClient) QueryStream(_ context.Context, _ log.Logger, _ *conn_clickhouse.Host, query any, params url.Values) (io.ReadCloser, error) {
	if queryText, ok := query.(string); ok && queryText == "SELECT 1 FORMAT Null" && params.Get("session_id") == "" && params.Get("query_id") == "" {
		return io.NopCloser(strings.NewReader("")), nil
	}
	err := c.query(params)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(strings.NewReader("")), nil
}

func newReadRetryClientFixture(test *testing.T) (*clientFixture, sqlmock.Sqlmock) {
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
	fixture := newClientFixture(test)
	configureTestNodes(fixture, func(nodes [][]Node) [][]Node {
		nodes[0][0].DB = database

		return nodes
	},
	)
	return fixture, mock
}

func TestCountPhysicalRowsUsesActivePartMetadata(test *testing.T) {
	for _, expectedRows := range []uint64{0, 257} {
		test.Run(strconv.FormatUint(expectedRows, 10), func(test *testing.T) {
			fixture, databaseMock := newReadRetryClientFixture(test)
			databaseMock.ExpectQuery(`SELECT sum\(rows\) FROM system.parts WHERE active AND database = \? AND table = \?`).
				WithArgs("test", "table'with-special-characters").
				WillReturnRows(sqlmock.NewRows([]string{"sum(rows)"}).AddRow(expectedRows))
			actualRows, err := fixture.tablesClient.CountPhysicalRows(context.Background(), "unused", "table'with-special-characters")
			require.NoError(test, err)
			require.Equal(test, expectedRows, actualRows)
		})
	}
}

func TestAvailableReplicasRechecksReturningHost(test *testing.T) {
	fixture, primary := newReadRetryClientFixture(test)
	configureTestNodes(fixture, func(nodes [][]Node) [][]Node {
		nodes[0][0].HTTP = newSessionHTTPClient(func(url.Values) error { return nil })

		return nodes
	},
	)
	database, secondary, err := sqlmock.New()
	require.NoError(test, err)
	defer database.Close()

	configureTestNodes(fixture, func(nodes [][]Node) [][]Node {
		nodes[0] = append(nodes[0], NewNode(&conn_clickhouse.Host{Name: "secondary"}, database, newSessionHTTPClient(func(url.Values) error { return nil })))

		return nodes
	},
	)
	fixture.tablesClient.SetReplicaFailureGrace(0)
	primary.ExpectQuery("SELECT toUInt8").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
	secondary.ExpectQuery("SELECT toUInt8").WillReturnError(io.ErrUnexpectedEOF)
	available, err := fixture.tablesClient.SelectReplicas(context.Background(), true)
	require.NoError(test, err)
	require.Len(test, available[0], 1)
	require.Len(test, fixture.tablesClient.NodesByShard()[0], 2)
	primary.ExpectQuery("SELECT toUInt8").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
	secondary.ExpectQuery("SELECT toUInt8").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
	available, err = fixture.tablesClient.SelectReplicas(context.Background(), true)
	require.NoError(test, err)
	require.Len(test, available[0], 2)
	require.NoError(test, secondary.ExpectationsWereMet())
}

func TestReplicaFailuresResetGraceAfterRecovery(test *testing.T) {
	fixture, databaseMock := newReadRetryClientFixture(test)
	failures := &ReplicaFailures{startTimes: sync.Map{}}
	failures.startTimes.Store("unused", time.Now().Add(-2*time.Hour))
	fixture.tablesClient.SetReplicaFailures(failures)
	databaseMock.ExpectQuery("SELECT toUInt8").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
	available, err := fixture.tablesClient.SelectReplicas(context.Background(), true)
	require.NoError(test, err)
	require.Len(test, available[0], 1)

	// Recovery starts a full grace period for a new failure, including in a new client.
	nextClient := NewClient(fixture.dstConfig, fixture.logger, fixture.tablesClient.NodesByShard())
	nextClient.SetReplicaFailures(failures)
	nextClient.SetReplicaFailureGrace(time.Hour)
	databaseMock.ExpectQuery("SELECT toUInt8").WillReturnError(io.ErrUnexpectedEOF)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	available, err = nextClient.SelectReplicas(ctx, true)
	require.ErrorIs(test, err, context.DeadlineExceeded)
	require.Nil(test, available)
}

func TestAvailableReplicasDoesNotMaskErrorsOrLoseShard(test *testing.T) {
	for _, serverCode := range []int32{0, 497, 60} {
		test.Run(fmt.Sprint(serverCode), func(test *testing.T) {
			fixture, mock := newReadRetryClientFixture(test)
			fixture.tablesClient.SetReplicaFailureGrace(0)
			failure := io.ErrUnexpectedEOF
			if serverCode != 0 {
				failure = &clickhouse_go.Exception{Code: serverCode, Message: "injected SQL error"}
			}
			mock.ExpectQuery("SELECT toUInt8").WillReturnError(failure)
			available, err := fixture.tablesClient.SelectReplicas(context.Background(), true)
			require.Error(test, err)
			require.Nil(test, available)
			if serverCode == 0 {
				require.ErrorContains(test, err, "no available replica")
			} else {
				require.ErrorIs(test, err, failure)
			}
		})
	}
}

func TestClientTopologyDoesNotExposeMutableNodes(test *testing.T) {
	fixture := newClientFixture(test)
	input := fixture.tablesClient.NodesByShard()
	client := NewClient(fixture.dstConfig, fixture.logger, input)
	input[0][0].Host.Name = "changed-input"
	input[0] = nil

	snapshot := client.NodesByShard()
	require.Equal(test, "unused", snapshot[0][0].Host.Name)
	snapshot[0][0].Host.Name = "changed-snapshot"
	snapshot[0][0].HTTP = nil
	snapshot[0] = nil

	node, err := client.NodeByHostName("unused")
	require.NoError(test, err)
	node.Host.Name = "changed-node"
	node.HTTP = nil
	available, err := client.SelectReplicas(context.Background(), false)
	require.NoError(test, err)
	available[0][0].Host.Name = "changed-available"
	require.Equal(test, "unused", client.NodesByShard()[0][0].Host.Name)
	require.NotNil(test, client.NodesByShard()[0][0].HTTP)
}

func TestSessionFenceRequiresConfirmedAbsence(test *testing.T) {
	for _, behavior := range []string{"absent", "unknown_setting", "ignored_close", "locked", "network_error"} {
		test.Run(behavior, func(test *testing.T) {
			// No native connection is used by the HTTP session protocol.
			fixture := newClientFixture(test)
			calls := 0
			configureTestNodes(fixture, func(nodes [][]Node) [][]Node {
				nodes[0][0].HTTP = newSessionHTTPClient(
					func(params url.Values) error {
						calls++
						require.Equal(test, "1", params.Get("session_check"))
						switch behavior {
						case "absent":
							return &clickhouse_go.Exception{Code: sessionNotFoundCode, Message: "Session not found"}
						case "unknown_setting":
							return &clickhouse_go.Exception{Code: 115, Message: "Unknown setting close_session"}
						case "locked":
							return &clickhouse_go.Exception{Code: sessionLockedCode, Message: "Session locked"}
						case "network_error":
							return xerrors.New("lost connection")
						default:
							return nil
						}
					},
				)

				return nodes
			},
			)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()

			err := fixture.tablesClient.FenceSession(ctx, "unused", "session")
			if behavior == "absent" {
				require.NoError(test, err)
			} else {
				require.Error(test, err)
			}
			expectedCalls := 1
			if behavior == "ignored_close" {
				// One immediate confirmation is safe. A successful confirmation
				// renews the session and must switch to the idle-timeout fallback.
				expectedCalls = 2
			}
			require.Equal(test, expectedCalls, calls)
		})
	}
}

func TestSessionSupportRejectsIgnoredSessionCheck(test *testing.T) {
	fixture := newClientFixture(test)
	calls := 0
	configureTestNodes(fixture, func(nodes [][]Node) [][]Node {
		nodes[0][0].HTTP = newSessionHTTPClient(
			func(url.Values) error {
				calls++
				return nil
			},
		)

		return nodes
	},
	)
	require.ErrorContains(test, fixture.tablesClient.CheckSessionFencingSupport(context.Background(), "unused"), "session_check must reject")
	require.Equal(test, 1, calls)
}

func TestSessionSupportRetriesTransportErrors(test *testing.T) {
	// Probe an absent session, open a new one, close it, then confirm absence.
	for failedStep := 0; failedStep < 4; failedStep++ {
		test.Run(strconv.Itoa(failedStep), func(test *testing.T) {
			fixture := newClientFixture(test)
			step := 0
			calls := 0
			var failedParams url.Values
			configureTestNodes(fixture, func(nodes [][]Node) [][]Node {
				nodes[0][0].HTTP = newSessionHTTPClient(
					func(params url.Values) error {
						calls++
						require.Empty(test, params.Get("query_id"))
						if step == failedStep && failedParams == nil {
							failedParams = params
							return &url.Error{Op: "Post", URL: "https://unused", Err: io.EOF}
						}
						if step == failedStep {
							if step == 1 {
								require.NotEqual(test, failedParams.Get("session_id"), params.Get("session_id"))
							} else {
								require.Equal(test, failedParams, params)
							}
						}
						step++
						switch step {
						case 1, 4:
							require.Equal(test, "1", params.Get("session_check"))
							return &clickhouse_go.Exception{Code: sessionNotFoundCode, Message: "Session not found"}
						case 2:
							require.Equal(test, "60", params.Get("session_timeout"))
						case 3:
							require.Equal(test, "1", params.Get("close_session"))
						default:
							test.Fatal("unexpected session probe")
						}
						return nil
					},
				)

				return nodes
			},
			)
			require.NoError(test, fixture.tablesClient.CheckSessionFencingSupport(context.Background(), "unused"))
			require.Equal(test, 5, calls)
			require.Equal(test, 4, step)
		})
	}
}

func TestSessionSupportLimitsRetriesAndPreservesError(test *testing.T) {
	for _, probeError := range []error{
		io.EOF,
		io.ErrUnexpectedEOF,
		&net.OpError{Op: "read", Net: "tcp", Err: io.EOF},
		&clickhouse_go.Exception{Code: 516, Message: "Authentication failed"},
	} {
		test.Run(probeError.Error(), func(test *testing.T) {
			fixture := newClientFixture(test)
			calls := 0
			configureTestNodes(fixture, func(nodes [][]Node) [][]Node {
				nodes[0][0].HTTP = newSessionHTTPClient(
					func(url.Values) error {
						calls++
						return probeError
					},
				)

				return nodes
			},
			)
			err := fixture.tablesClient.CheckSessionFencingSupport(context.Background(), "unused")
			require.ErrorIs(test, err, probeError)
			require.ErrorContains(test, err, "unable to confirm")
			expectedCalls := 3
			if _, isServerError := probeError.(*clickhouse_go.Exception); isServerError {
				expectedCalls = 1
			}
			require.Equal(test, expectedCalls, calls)
		})
	}
}

func TestSessionProbeCancellationStopsRetries(test *testing.T) {
	fixture := newClientFixture(test)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	calls := 0
	configureTestNodes(fixture, func(nodes [][]Node) [][]Node {
		nodes[0][0].HTTP = newSessionHTTPClient(
			func(url.Values) error {
				calls++
				cancel()
				return ctx.Err()
			},
		)

		return nodes
	},
	)
	require.ErrorIs(test, fixture.tablesClient.CheckSessionFencingSupport(ctx, "unused"), context.Canceled)
	require.Equal(test, 1, calls)
	require.ErrorIs(test, fixture.tablesClient.CheckSessionFencingSupport(ctx, "unused"), context.Canceled)
	require.Equal(test, 1, calls)
}

func TestStagingSessionCreationNeverRetriesSameID(test *testing.T) {
	fixture := newClientFixture(test)
	// Session bootstrap must work before the database factory selects a strategy.
	calls := 0
	configureTestNodes(fixture, func(nodes [][]Node) [][]Node {
		nodes[0][0].HTTP = newSessionHTTPClient(
			func(params url.Values) error {
				calls++
				require.Equal(test, "bootstrap", params.Get("session_id"))
				require.Empty(test, params.Get("session_check"))
				require.Empty(test, params.Get("query_id"))
				return io.EOF
			},
		)

		return nodes
	},
	)
	require.ErrorIs(test, fixture.tablesClient.ExecuteCommandOverHTTP(context.Background(), "unused", "SELECT 1 FORMAT Null", url.Values{"session_id": {"bootstrap"}, "session_timeout": {"3600"}}), io.EOF)
	require.Equal(test, 1, calls)
}

func TestReplicaSynchronizationRetriesTransportErrors(test *testing.T) {
	for _, statement := range []string{"SYSTEM SYNC REPLICA `test`.`events`", "SYSTEM SYNC DATABASE REPLICA `test`"} {
		test.Run(statement, func(test *testing.T) {
			fixture := newClientFixture(test)
			calls := 0
			configureTestNodes(fixture, func(nodes [][]Node) [][]Node {
				nodes[0][0].HTTP = newSessionHTTPClient(
					func(url.Values) error {
						calls++
						if calls == 1 {
							return io.EOF
						}
						return nil
					},
				)

				return nodes
			},
			)
			require.NoError(test, fixture.tablesClient.ExecuteCommandOverHTTP(context.Background(), "unused", statement, nil))
			require.Equal(test, 2, calls)
		})
	}
}

func TestReplicaSynchronizationRetriesServerWaitTimeout(test *testing.T) {
	for _, statement := range []string{"SYSTEM SYNC REPLICA `test`.`events`", "SYSTEM SYNC DATABASE REPLICA `test`"} {
		test.Run(statement, func(test *testing.T) {
			fixture := newClientFixture(test)
			calls := 0
			configureTestNodes(fixture, func(nodes [][]Node) [][]Node {
				nodes[0][0].HTTP = newSessionHTTPClient(func(url.Values) error {
					calls++
					if calls <= 4 {
						return &clickhouse_go.Exception{Code: 159, Message: "SYNC REPLICA: command timed out. See the 'receive_timeout' setting"}
					}
					return nil
				})
				return nodes
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			require.NoError(test, fixture.tablesClient.ExecuteCommandOverHTTP(ctx, "unused", statement, nil))
			require.Equal(test, 5, calls)
		})
	}
}

func TestReplicaSynchronizationServerWaitTimeoutRespectsCancellation(test *testing.T) {
	for _, statement := range []string{"SYSTEM SYNC REPLICA `test`.`events`", "SYSTEM SYNC DATABASE REPLICA `test`"} {
		test.Run(statement, func(test *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fixture := newClientFixture(test)
			calls := 0
			configureTestNodes(fixture, func(nodes [][]Node) [][]Node {
				nodes[0][0].HTTP = newSessionHTTPClient(func(url.Values) error {
					calls++
					cancel()
					return &clickhouse_go.Exception{Code: 159, Message: "SYNC REPLICA: command timed out"}
				})
				return nodes
			})

			require.ErrorIs(test, fixture.tablesClient.ExecuteCommandOverHTTP(ctx, "unused", statement, nil), context.Canceled)
			require.Equal(test, 1, calls)
		})
	}
}

func TestHTTPCommandsDoNotRetryOtherServerErrors(test *testing.T) {
	for _, testCase := range []struct {
		statement string
		params    url.Values
		code      int32
	}{
		{statement: "SYSTEM SYNC REPLICA `test`.`events`", params: nil, code: 516},
		{statement: "SYSTEM SYNC DATABASE REPLICA `test`", params: nil, code: 516},
		{statement: "INSERT INTO `test`.`events` VALUES (1)", params: nil, code: 159},
		{statement: "DROP TABLE `test`.`events` SYNC", params: url.Values{"query_id": {"owned-command"}}, code: 159},
		{statement: "SELECT 1 FORMAT Null", params: ExistingSessionParams("probe"), code: 159},
	} {
		test.Run(testCase.statement, func(test *testing.T) {
			fixture := newClientFixture(test)
			calls := 0
			serverError := &clickhouse_go.Exception{Code: testCase.code, Message: "server error"}
			configureTestNodes(fixture, func(nodes [][]Node) [][]Node {
				nodes[0][0].HTTP = newSessionHTTPClient(func(url.Values) error {
					calls++
					return serverError
				})
				return nodes
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			require.ErrorIs(test, fixture.tablesClient.ExecuteCommandOverHTTP(ctx, "unused", testCase.statement, testCase.params), serverError)
			require.Equal(test, 1, calls)
		})
	}
}

type contextHTTPClient struct {
	httpclient.HTTPClient
	query func(context.Context) error
}

var _ httpclient.HTTPClient = (*contextHTTPClient)(nil)

func (c *contextHTTPClient) QueryStream(ctx context.Context, _ log.Logger, _ *conn_clickhouse.Host, _ any, _ url.Values) (io.ReadCloser, error) {
	if err := c.query(ctx); err != nil {
		return nil, err
	}
	return io.NopCloser(strings.NewReader("")), nil
}

func TestReplicaSynchronizationUsesOperationDeadline(test *testing.T) {
	for _, statement := range []string{"SYSTEM SYNC REPLICA `test`.`events`", "SYSTEM SYNC DATABASE REPLICA `test`"} {
		for _, withDeadline := range []bool{false, true} {
			test.Run(fmt.Sprintf("%s/deadline=%t", statement, withDeadline), func(test *testing.T) {
				ctx := context.Background()
				if withDeadline {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, 2*time.Hour)
					test.Cleanup(cancel)
				}
				deadline, hasDeadline := ctx.Deadline()
				fixture := newClientFixture(test)
				configureTestNodes(fixture, func(nodes [][]Node) [][]Node {
					nodes[0][0].HTTP = &contextHTTPClient{
						HTTPClient: nil,
						query: func(queryContext context.Context) error {
							queryDeadline, queryHasDeadline := queryContext.Deadline()
							require.Equal(test, hasDeadline, queryHasDeadline)
							require.Equal(test, deadline, queryDeadline)
							return nil
						},
					}
					return nodes
				})
				require.NoError(test, fixture.tablesClient.ExecuteCommandOverHTTP(ctx, "unused", statement, nil))
			})
		}
	}
}

func TestReplicaSynchronizationCancellationStopsWaitAndRetries(test *testing.T) {
	for _, statement := range []string{"SYSTEM SYNC REPLICA `test`.`events`", "SYSTEM SYNC DATABASE REPLICA `test`"} {
		test.Run(statement, func(test *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			fixture := newClientFixture(test)
			configureTestNodes(fixture, func(nodes [][]Node) [][]Node {
				nodes[0][0].HTTP = &contextHTTPClient{
					HTTPClient: nil,
					query: func(queryContext context.Context) error {
						calls++
						cancel()
						select {
						case <-queryContext.Done():
							return queryContext.Err()
						case <-time.After(time.Second):
							return xerrors.New("SYNC did not receive operation cancellation")
						}
					},
				}
				return nodes
			})
			require.ErrorIs(test, fixture.tablesClient.ExecuteCommandOverHTTP(ctx, "unused", statement, nil), context.Canceled)
			require.Equal(test, 1, calls)
		})
	}
}

func TestHTTPCommandsKeepExistingDeadlineRules(test *testing.T) {
	for _, testCase := range []struct {
		name         string
		statement    string
		params       url.Values
		probeTimeout bool
	}{
		{name: "probe", statement: "SELECT 1 FORMAT Null", params: nil, probeTimeout: true},
		{name: "session bootstrap", statement: "SELECT 1 FORMAT Null", params: url.Values{"session_id": {"bootstrap"}}, probeTimeout: true},
		{name: "command without query ID", statement: "DROP TABLE `test`.`events` SYNC", params: nil, probeTimeout: true},
		{name: "owned command", statement: "DROP TABLE `test`.`events` SYNC", params: url.Values{"query_id": {"owned-command"}}, probeTimeout: false},
	} {
		test.Run(testCase.name, func(test *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
			defer cancel()
			operationDeadline, _ := ctx.Deadline()
			fixture := newClientFixture(test)
			configureTestNodes(fixture, func(nodes [][]Node) [][]Node {
				nodes[0][0].HTTP = &contextHTTPClient{
					HTTPClient: nil,
					query: func(queryContext context.Context) error {
						deadline, hasDeadline := queryContext.Deadline()
						require.True(test, hasDeadline)
						if testCase.probeTimeout {
							require.Positive(test, time.Until(deadline))
							require.LessOrEqual(test, time.Until(deadline), time.Minute)
						} else {
							require.Equal(test, operationDeadline, deadline)
						}
						return nil
					},
				}
				return nodes
			})
			require.NoError(test, fixture.tablesClient.ExecuteCommandOverHTTP(ctx, "unused", testCase.statement, testCase.params))
		})
	}
}

func TestHTTPProbeRetriesProxyAndTruncatedResponses(test *testing.T) {
	for _, failureKind := range []string{"502", "503", "504", "truncated_body", "authentication"} {
		test.Run(failureKind, func(test *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(
				http.HandlerFunc(
					func(writer http.ResponseWriter, _ *http.Request) {
						if calls.Add(1) != 1 {
							return
						}
						switch failureKind {
						case "truncated_body":
							writer.Header().Set("Content-Length", "100")
							_, _ = io.WriteString(writer, "incomplete")
						case "authentication":
							writer.WriteHeader(http.StatusServiceUnavailable)
							_, _ = io.WriteString(writer, "Code: 516. DB::Exception: Authentication failed (AUTHENTICATION_FAILED)")
						default:
							statusCode, _ := strconv.Atoi(failureKind)
							writer.WriteHeader(statusCode)
							_, _ = io.WriteString(writer, "proxy is unavailable")
						}
					},
				),
			)
			defer server.Close()

			fixture := newClientFixture(test)
			params, err := fixture.dstConfig.ToSinkParams(&model.Transfer{})
			require.NoError(test, err)
			client, err := httpclient.NewHTTPClientImpl(params)
			require.NoError(test, err)
			configureTestNodes(fixture, func(nodes [][]Node) [][]Node {
				nodes[0][0].HTTP = client

				return nodes
			},
			)
			configureTestNodes(fixture, func(nodes [][]Node) [][]Node {
				nodes[0][0].Host = &conn_clickhouse.Host{Name: "localhost", HTTPPort: server.Listener.Addr().(*net.TCPAddr).Port}

				return nodes
			},
			)
			err = fixture.tablesClient.ExecuteCommandOverHTTP(context.Background(), "localhost", "SELECT 1 FORMAT Null", nil)
			if failureKind == "authentication" {
				var serverError *clickhouse_go.Exception
				require.ErrorAs(test, err, &serverError)
				require.EqualValues(test, 516, serverError.Code)
				require.EqualValues(test, 1, calls.Load())
				return
			}
			require.NoError(test, err)
			require.EqualValues(test, 2, calls.Load())
		})
	}
}

func TestSessionFenceConfirmsClosureImmediately(test *testing.T) {
	fixture := newClientFixture(test)
	calls := 0
	configureTestNodes(fixture, func(nodes [][]Node) [][]Node {
		nodes[0][0].HTTP = newSessionHTTPClient(
			func(params url.Values) error {
				calls++
				if calls == 1 {
					require.Equal(test, "1", params.Get("close_session"))
					return nil
				}
				require.Empty(test, params.Get("close_session"))
				return &clickhouse_go.Exception{Code: sessionNotFoundCode, Message: "Session not found"}
			},
		)

		return nodes
	},
	)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	require.NoError(test, fixture.tablesClient.FenceSession(ctx, "unused", "session"))
	require.Equal(test, 2, calls)
}

func TestSessionFenceRechecksLockedSession(test *testing.T) {
	fixture := newClientFixture(test)
	calls := 0
	configureTestNodes(fixture, func(nodes [][]Node) [][]Node {
		nodes[0][0].HTTP = newSessionHTTPClient(
			func(params url.Values) error {
				calls++
				if calls == 1 {
					return &clickhouse_go.Exception{Code: sessionLockedCode, Message: "Session locked"}
				}
				if calls == 2 {
					require.Equal(test, "1", params.Get("close_session"))
					return nil
				}
				return &clickhouse_go.Exception{Code: sessionNotFoundCode, Message: "Session not found"}
			},
		)

		return nodes
	},
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(test, fixture.tablesClient.FenceSession(ctx, "unused", "session"))
	require.Equal(test, 3, calls)
}
