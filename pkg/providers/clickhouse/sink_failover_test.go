package clickhouse

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/blang/semver/v4"
	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	chconn "github.com/transferia/transferia/pkg/connection/clickhouse"
	chmodel "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/stats"
)

func newHTTPFailoverResponse(t *testing.T, status int, body string, requests *atomic.Int32) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests != nil {
			requests.Add(1)
		}
		w.WriteHeader(status)
		_, _ = fmt.Fprint(w, body)
	}))
	t.Cleanup(server.Close)
	return server
}

type refreshingSinkParams struct {
	chmodel.ChSinkShardParams
	resolve func() ([]*chconn.Host, error)
}

func sinkFailoverParams(t *testing.T, host *chconn.Host) chmodel.ChDestinationWrapper {
	t.Helper()
	params, err := (&chmodel.ChDestination{
		Database: "default", ForceJSONMode: true, IsSchemaMigrationDisabled: true,
		ShardsList: []chmodel.ClickHouseShard{{Name: "shard", Hosts: []string{host.Name}}}, HTTPPort: host.HTTPPort,
	}).ToSinkParams(&dp_model.Transfer{})
	require.NoError(t, err)
	return params
}

func newSinkRecoveryShard(t *testing.T, config chmodel.ChSinkShardParams, cluster *sinkCluster, factory func(map[string]time.Time) (*sinkCluster, error), retries uint64) *sinkShard {
	t.Helper()
	shard := &sinkShard{
		config: config, cluster: cluster, logger: logger.Log, metrics: stats.NewSinkerStats(emptyRegistry()), newClusterForTests: factory,
		retryFunc: func(f func() error) error {
			return backoff.Retry(f, backoff.WithMaxRetries(backoff.NewConstantBackOff(0), retries))
		},
	}
	t.Cleanup(func() { _ = shard.Close() })
	return shard
}

// Initial discovery succeeds, but any later MDB request would hang and fail.
// With no write failure to recover from, the sink must write without waiting for discovery.
func TestSinkHealthyPushSucceedsWhenDiscoveryIsUnavailable(t *testing.T) {
	var writes, resolutions atomic.Int32
	endpoint := newHTTPFailoverResponse(t, http.StatusOK, "", &writes)
	host := &chconn.Host{Name: "127.0.0.1", HTTPPort: endpoint.Listener.Addr().(*net.TCPAddr).Port}
	params := sinkFailoverParams(t, host)
	discoveryStarted, releaseDiscovery := make(chan struct{}), make(chan struct{})
	var releaseOnce, discoveryOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseDiscovery) }) }
	t.Cleanup(release)
	config := refreshingSinkParams{ChSinkShardParams: params, resolve: func() ([]*chconn.Host, error) {
		// Construction may resolve hosts; later calls model an unavailable MDB API.
		if resolutions.Add(1) == 1 {
			return []*chconn.Host{host}, nil
		}
		discoveryOnce.Do(func() { close(discoveryStarted) })
		<-releaseDiscovery
		return nil, xerrors.New("MDB discovery unavailable")
	}}
	factory := sinkRecoveryFactory(t, config, nil)
	cluster, err := factory(nil)
	require.NoError(t, err)
	// Exercise periodic maintenance without waiting for its production interval.
	go cluster.pool.reviveDeadHosts(nil, time.Millisecond)
	select {
	case <-discoveryStarted:
	case <-time.After(50 * time.Millisecond):
		// A pool that only revives its known startup hosts has no discovery work.
	}
	shard := newSinkRecoveryShard(t, config, cluster, nil, 0)
	cols := abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "id", DataType: "int64", Required: true}})
	done := make(chan error, 1)
	// The write must finish before cleanup releases a blocked discovery request.
	go func() {
		done <- shard.Push([]abstract.ChangeItem{{Kind: abstract.InsertKind, Table: "test_table", ColumnNames: []string{"id"}, ColumnValues: []any{int64(1)}, TableSchema: cols}})
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(250 * time.Millisecond):
		t.Error("healthy Push waited for background discovery before attempting its write")
		release()
		<-done
	}
	require.Equal(t, int32(1), writes.Load(), "a healthy sink must attempt its write despite discovery failure")
	release()
}

func (p refreshingSinkParams) ResolveHosts(context.Context) ([]*chconn.Host, error) {
	return p.resolve()
}

// The factory supplies SQL metadata responses instead of a native server. Host
// discovery calls, cluster construction, pool selection, retries, and HTTP writes stay in production code.
func sinkRecoveryFactory(t *testing.T, config chmodel.ChSinkClusterParams, connect func(*chconn.Host) error) func(map[string]time.Time) (*sinkCluster, error) {
	t.Helper()
	return func(failedAt map[string]time.Time) (*sinkCluster, error) {
		return newSinkClusterWithServerFactory(config, logger.Log, nil, failedAt, func(host *chconn.Host, cluster *sinkCluster) (*SinkServer, error) {
			if connect != nil {
				if err := connect(host); err != nil {
					return nil, err
				}
			}
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			mock.ExpectQuery("select count").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
			mock.ExpectQuery("select name, type from system.columns").WillReturnRows(sqlmock.NewRows([]string{"name", "type"}).AddRow("id", "Int64"))
			// SQL Close observes cluster retirement; HTTP handlers count write attempts.
			mock.ExpectClose()
			server := NewSinkServerImpl(config.MakeChildServerParams(host), db, semver.Version{Major: 25}, time.UTC, logger.Log, stats.NewChStats(emptyRegistry()), cluster)
			t.Cleanup(func() {
				require.NoError(t, server.Close())
				require.NoError(t, mock.ExpectationsWereMet())
			})
			return server, nil
		})
	}
}

// A fails its first write but would accept later writes, making stale reuse visible.
// Failed discovery or connection setup must prevent writes in later Push calls until B is ready.
func TestSinkShardRecoveryBlocksStaleWritesUntilReplacementSucceeds(t *testing.T) {
	for _, failure := range []string{"resolve", "connect"} {
		t.Run(failure, func(t *testing.T) {
			var failedWrites, healthyWrites atomic.Int32
			failedEndpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if failedWrites.Add(1) == 1 {
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte("Code: 667. DB::Exception: Table is not initialized yet."))
				}
				// A later write would succeed, exposing a stale acknowledgement.
			}))
			t.Cleanup(failedEndpoint.Close)
			healthyEndpoint := newHTTPFailoverResponse(t, http.StatusOK, "", &healthyWrites)
			failed := &chconn.Host{Name: "127.0.0.1", HTTPPort: failedEndpoint.Listener.Addr().(*net.TCPAddr).Port, ShardName: "shard"}
			healthy := &chconn.Host{Name: "127.0.0.1", HTTPPort: healthyEndpoint.Listener.Addr().(*net.TCPAddr).Port, ShardName: "shard"}
			params := sinkFailoverParams(t, failed)
			// replaced switches discovery from A to B; broken prevents B's recovery step.
			var replaced, broken atomic.Bool
			recoveryErr := xerrors.New("replica recovery unavailable")
			config := refreshingSinkParams{ChSinkShardParams: params, resolve: func() ([]*chconn.Host, error) {
				if !replaced.Load() {
					return []*chconn.Host{failed}, nil
				}
				if failure == "resolve" && broken.Load() {
					return nil, recoveryErr
				}
				return []*chconn.Host{healthy}, nil
			}}
			factory := sinkRecoveryFactory(t, config, func(host *chconn.Host) error {
				if failure == "connect" && broken.Load() {
					return recoveryErr
				}
				return nil
			})
			original, err := factory(nil)
			require.NoError(t, err)
			shard := newSinkRecoveryShard(t, config, original, factory, 0)
			cols := abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "id", DataType: "int64", Required: true}})
			batch := []abstract.ChangeItem{{Kind: abstract.InsertKind, Table: "test_table", ColumnNames: []string{"id"}, ColumnValues: []any{int64(1)}, TableSchema: cols}}

			// Retries are disabled here so each Push exposes one stage of recovery.
			require.Error(t, shard.Push(batch))
			require.True(t, shard.needsReset)
			// MDB now points to B, but this subcase still fails discovery or connection setup.
			replaced.Store(true)
			broken.Store(true)
			require.ErrorIs(t, shard.Push(batch), recoveryErr)
			require.True(t, shard.needsReset, "a failed replacement must remain mandatory for the next Push")
			require.Same(t, original, shard.cluster)
			select {
			case <-original.pool.closeCh:
				t.Fatal("failed replacement closed the previous cluster")
			default:
			}
			require.Equal(t, int32(1), failedWrites.Load(), "failed recovery must not acknowledge a write through the stale endpoint")
			require.Zero(t, healthyWrites.Load())

			// Build B's cluster successfully, retire A, then deliver the batch to B.
			broken.Store(false)
			require.NoError(t, shard.Push(batch))
			require.False(t, shard.needsReset)
			require.NotSame(t, original, shard.cluster)
			select {
			case <-original.pool.closeCh:
			default:
				t.Fatal("successful replacement did not close the previous cluster")
			}
			require.Equal(t, int32(1), failedWrites.Load())
			require.Equal(t, int32(1), healthyWrites.Load())
		})
	}
}

// A rejects the batch with HTTP 667. Within the same Push, its retry must discover
// B, replace the cluster, and write to B instead of retrying the retired endpoint.
func TestSinkShardPushRetriesFailedWriteOnReplacementReplica(t *testing.T) {
	var failedWrites, healthyWrites atomic.Int32
	failedEndpoint := newHTTPFailoverResponse(t, http.StatusInternalServerError, "Code: 667. DB::Exception: Table is not initialized yet.", &failedWrites)
	healthyEndpoint := newHTTPFailoverResponse(t, http.StatusOK, "", &healthyWrites)
	failed := &chconn.Host{Name: "127.0.0.1", HTTPPort: failedEndpoint.Listener.Addr().(*net.TCPAddr).Port, ShardName: "shard"}
	healthy := &chconn.Host{Name: "127.0.0.1", HTTPPort: healthyEndpoint.Listener.Addr().(*net.TCPAddr).Port, ShardName: "shard"}
	params := sinkFailoverParams(t, failed)
	config := refreshingSinkParams{ChSinkShardParams: params, resolve: func() ([]*chconn.Host, error) {
		// Advertise B only after a real HTTP write has reached and failed on A.
		if failedWrites.Load() == 0 {
			return []*chconn.Host{failed}, nil
		}
		return []*chconn.Host{healthy}, nil
	}}
	factory := sinkRecoveryFactory(t, config, nil)
	cluster, err := factory(nil)
	require.NoError(t, err)
	// Allow one retry inside Push, unlike the cross-Push recovery test above.
	shard := newSinkRecoveryShard(t, config, cluster, factory, 1)
	cols := abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "id", DataType: "int64", Required: true}})
	err = shard.Push([]abstract.ChangeItem{{Kind: abstract.InsertKind, Table: "test_table", ColumnNames: []string{"id"}, ColumnValues: []any{int64(1)}, TableSchema: cols}})

	require.NoError(t, err)
	require.Equal(t, int32(1), failedWrites.Load())
	require.Equal(t, int32(1), healthyWrites.Load())
	require.False(t, shard.needsReset)
}

func TestSinkShardRetriesBackpressureWithoutRediscovery(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "202", body: "Code: 202. DB::Exception: Too many simultaneous queries."},
		{name: "252 merge backlog", body: "Code: 252. DB::Exception: Too many parts. Merges are processing significantly slower than inserts."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var writes, resolutions atomic.Int32
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if writes.Add(1) == 1 {
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = fmt.Fprint(w, tc.body)
				}
			}))
			t.Cleanup(endpoint.Close)
			host := &chconn.Host{Name: "127.0.0.1", HTTPPort: endpoint.Listener.Addr().(*net.TCPAddr).Port, ShardName: "shard"}
			params := sinkFailoverParams(t, host)
			config := refreshingSinkParams{ChSinkShardParams: params, resolve: func() ([]*chconn.Host, error) {
				if resolutions.Add(1) > 1 {
					return nil, xerrors.New("MDB discovery unavailable")
				}
				return []*chconn.Host{host}, nil
			}}
			factory := sinkRecoveryFactory(t, config, nil)
			cluster, err := factory(nil)
			require.NoError(t, err)
			shard := newSinkRecoveryShard(t, config, cluster, factory, 1)
			cols := abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "id", DataType: "int64", Required: true}})

			err = shard.Push([]abstract.ChangeItem{{Kind: abstract.InsertKind, Table: "test_table", ColumnNames: []string{"id"}, ColumnValues: []any{int64(1)}, TableSchema: cols}})

			require.NoError(t, err, "transient backpressure must not make the next write depend on MDB availability")
			require.Equal(t, int32(2), writes.Load())
			require.Equal(t, int32(1), resolutions.Load(), "initial discovery must be sufficient for the retry")
			require.Same(t, cluster, shard.cluster)
			require.False(t, shard.needsReset)
		})
	}
}

// Both replicas remain discoverable and native metadata queries succeed on both.
// A consistently rejects HTTP INSERT, so rebuilding the pool must keep A out of
// rotation for later batches while B continues accepting every batch.
func TestSinkShardRecoveryPreservesFailedReplicaCooldownAcrossBatches(t *testing.T) {
	var failedWrites, healthyWrites atomic.Int32
	failedEndpoint := newHTTPFailoverResponse(t, http.StatusInternalServerError, "Code: 667. DB::Exception: Table is not initialized yet.", &failedWrites)
	healthyEndpoint := newHTTPFailoverResponse(t, http.StatusOK, "", &healthyWrites)
	failed := &chconn.Host{Name: "127.0.0.1", HTTPPort: failedEndpoint.Listener.Addr().(*net.TCPAddr).Port, ShardName: "shard"}
	healthy := &chconn.Host{Name: "127.0.0.1", HTTPPort: healthyEndpoint.Listener.Addr().(*net.TCPAddr).Port, ShardName: "shard"}
	params := sinkFailoverParams(t, failed)
	config := refreshingSinkParams{ChSinkShardParams: params, resolve: func() ([]*chconn.Host, error) {
		return []*chconn.Host{failed, healthy}, nil
	}}
	cols := abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "id", DataType: "int64", Required: true}})
	create := sinkRecoveryFactory(t, config, nil)
	builds := 0
	factory := func(failedAt map[string]time.Time) (*sinkCluster, error) {
		cluster, err := create(failedAt)
		if err != nil {
			return nil, err
		}
		// Resolve native metadata on both replicas, including the one whose HTTP
		// writes fail. The native boundary must not make A look unreachable.
		for _, server := range cluster.pool.serversCopy() {
			_, err := server.GetTable("test_table", cols)
			require.NoError(t, err)
		}
		// Remove the pool's startup shuffle from this scenario: first choose A;
		// after reset choose B, then let the real round-robin run across batches.
		first := healthy
		if builds == 0 {
			first = failed
		}
		builds++
		cluster.pool.mu.Lock()
		if cluster.pool.servers[0].config.Host().HTTPPort != first.HTTPPort {
			cluster.pool.servers[0], cluster.pool.servers[1] = cluster.pool.servers[1], cluster.pool.servers[0]
		}
		cluster.pool.mu.Unlock()
		return cluster, nil
	}
	cluster, err := factory(nil)
	require.NoError(t, err)
	shard := newSinkRecoveryShard(t, config, cluster, factory, 1)
	const batches = 20
	for batchID := range batches {
		err := shard.Push([]abstract.ChangeItem{{Kind: abstract.InsertKind, Table: "test_table", ColumnNames: []string{"id"}, ColumnValues: []any{int64(batchID)}, TableSchema: cols}})
		require.NoError(t, err)
	}

	require.Equal(t, int32(batches), healthyWrites.Load(), "every batch must reach the healthy replica")
	require.Equal(t, int32(1), failedWrites.Load(), "reset must preserve the failed endpoint's five-minute write cooldown")
	for _, server := range shard.cluster.pool.serversCopy() {
		require.Equal(t, server.config.Host().HTTPPort == healthy.HTTPPort, server.Alive(), "cooldown must distinguish endpoints with the same hostname and different HTTP ports")
	}
}

// A first fails HTTP INSERT, then becomes unreachable through its native port.
// Two successful pool replacements contain only B. When A's native connection
// later recovers, publication by the real revive loop must retain A's HTTP cooldown.
func TestSinkShardRecoveryKeepsCooldownForReplicaRevivedAfterMultipleResets(t *testing.T) {
	var failedWrites, healthyWrites atomic.Int32
	var rejectHealthyWrite atomic.Bool
	failedEndpoint := newHTTPFailoverResponse(t, http.StatusInternalServerError, "Code: 667. DB::Exception: Table is not initialized yet.", &failedWrites)
	healthyEndpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		healthyWrites.Add(1)
		if rejectHealthyWrite.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("Code: 667. DB::Exception: Table is not initialized yet."))
		}
	}))
	t.Cleanup(healthyEndpoint.Close)
	failed := &chconn.Host{Name: "127.0.0.1", HTTPPort: failedEndpoint.Listener.Addr().(*net.TCPAddr).Port, ShardName: "shard"}
	healthy := &chconn.Host{Name: "127.0.0.1", HTTPPort: healthyEndpoint.Listener.Addr().(*net.TCPAddr).Port, ShardName: "shard"}
	params := sinkFailoverParams(t, failed)
	config := refreshingSinkParams{ChSinkShardParams: params, resolve: func() ([]*chconn.Host, error) {
		return []*chconn.Host{failed, healthy}, nil
	}}
	var nativeUnavailable atomic.Bool
	create := sinkRecoveryFactory(t, config, func(host *chconn.Host) error {
		if host.String() == failed.String() && nativeUnavailable.Load() {
			return xerrors.New("native connection temporarily unavailable")
		}
		return nil
	})
	var snapshots []map[string]time.Time
	factory := func(failedAt map[string]time.Time) (*sinkCluster, error) {
		snapshots = append(snapshots, failedAt)
		return create(failedAt)
	}
	cluster, err := factory(nil)
	require.NoError(t, err)
	shard := newSinkRecoveryShard(t, config, cluster, factory, 0)
	shard.lastFailures = map[string]time.Time{"expired endpoint": time.Now().Add(-6 * time.Minute)}
	cols := abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "id", DataType: "int64", Required: true}})
	primeMetadata := func() {
		for _, server := range shard.cluster.pool.serversCopy() {
			_, err := server.GetTable("test_table", cols)
			require.NoError(t, err)
		}
	}
	primeMetadata()
	// Control only the initial shuffle so the first real write reaches A.
	cluster.pool.mu.Lock()
	if cluster.pool.servers[0].config.Host().String() != failed.String() {
		cluster.pool.servers[0], cluster.pool.servers[1] = cluster.pool.servers[1], cluster.pool.servers[0]
	}
	cluster.pool.mu.Unlock()
	batch := []abstract.ChangeItem{{Kind: abstract.InsertKind, Table: "test_table", ColumnNames: []string{"id"}, ColumnValues: []any{int64(1)}, TableSchema: cols}}
	require.Error(t, shard.Push(batch))
	require.Equal(t, int32(1), failedWrites.Load())

	nativeUnavailable.Store(true)
	for range 2 {
		require.NoError(t, shard.reset())
		require.Len(t, shard.cluster.pool.serversCopy(), 1, "A is still discoverable but cannot enter the connected pool")
		primeMetadata()
	}
	nativeUnavailable.Store(false)
	// Use the production revive loop with a short interval; its regular loop
	// remains at one minute and does not need to fire during this test.
	go shard.cluster.pool.reviveDeadHosts([]*chconn.Host{failed}, time.Millisecond)
	require.Eventually(t, func() bool { return len(shard.cluster.pool.serversCopy()) == 2 }, 5*time.Second, time.Millisecond)
	primeMetadata()
	for _, server := range shard.cluster.pool.serversCopy() {
		if server.config.Host().String() == failed.String() {
			require.False(t, server.Alive(), "native revival must not clear the unexpired HTTP failure cooldown")
		} else {
			require.True(t, server.Alive(), "an endpoint with no HTTP failure must remain available")
		}
	}
	for range 10 {
		require.NoError(t, shard.Push(batch), "every later batch must keep using healthy B")
	}
	require.Equal(t, int32(1), failedWrites.Load())
	require.Equal(t, int32(10), healthyWrites.Load())
	require.NotContains(t, shard.lastFailures, "expired endpoint", "expired cooldown entries must not accumulate across replacements")

	// A later real failure on B belongs only to the next build's snapshot.
	// Previous factories can still be running native revival during reset.
	rejectHealthyWrite.Store(true)
	require.Error(t, shard.Push(batch))
	require.NoError(t, shard.reset())
	primeMetadata()
	require.NotContains(t, snapshots[2], healthy.String(), "reset must not mutate a snapshot captured by the previous pool")
	require.Contains(t, snapshots[3], healthy.String())
}

// reset performs one rebuild attempt. Discovery failure must leave the old cluster
// open and recovery mandatory; the outer Push loop owns any retry.
func TestSinkShardResetKeepsOldClusterWhenDiscoveryFails(t *testing.T) {
	resolveErr := xerrors.New("all ClickHouse replicas are unavailable")
	var calls atomic.Int32
	config := refreshingSinkParams{resolve: func() ([]*chconn.Host, error) {
		calls.Add(1)
		return nil, resolveErr
	}}
	cluster := &sinkCluster{pool: &serverPool{closeCh: make(chan struct{}), logger: logger.Log}}
	t.Cleanup(func() { _ = cluster.Close() })
	shard := &sinkShard{config: config, cluster: cluster, logger: logger.Log, needsReset: true}

	require.ErrorIs(t, shard.reset(), resolveErr)
	require.Equal(t, int32(1), calls.Load(), "only the outer Push loop owns retries")
	require.Same(t, cluster, shard.cluster)
	require.True(t, shard.needsReset)
	select {
	case <-cluster.pool.closeCh:
		t.Fatal("resolve failure closed the previous cluster")
	default:
	}
}

// A missing cluster is a permanent discovery error, even when Push has retries left.
func TestSinkShardRecoveryDoesNotRetryFatalDiscoveryError(t *testing.T) {
	resolveErr := abstract.NewFatalError(xerrors.New("cluster does not exist"))
	var calls atomic.Int32
	config := refreshingSinkParams{resolve: func() ([]*chconn.Host, error) {
		calls.Add(1)
		return nil, resolveErr
	}}
	cluster := &sinkCluster{pool: &serverPool{closeCh: make(chan struct{}), logger: logger.Log}}
	t.Cleanup(func() { _ = cluster.Close() })
	shard := &sinkShard{
		config: config, cluster: cluster, logger: logger.Log, needsReset: true,
		retryFunc: func(f func() error) error {
			return backoff.Retry(f, backoff.WithMaxRetries(backoff.NewConstantBackOff(0), 2))
		},
	}

	err := shard.Push(nil)
	require.ErrorIs(t, err, resolveErr)
	require.True(t, abstract.IsFatal(err))
	require.Equal(t, int32(1), calls.Load(), "fatal discovery errors must not be retried")
	require.True(t, shard.needsReset)
}

// Native ClickHouse error 62 is a syntax error, not a failed replica to rediscover.
// Push must preserve its fatal classification and stop without rebuilding the cluster.
func TestSinkShardPushDoesNotRetryFatalNativeInsertError(t *testing.T) {
	host := &chconn.Host{Name: "replica"}
	params, err := (&chmodel.ChDestination{
		Database: "default", IsSchemaMigrationDisabled: true,
		ShardsList: []chmodel.ClickHouseShard{{Name: "shard", Hosts: []string{host.Name}}},
	}).ToSinkParams(&dp_model.Transfer{})
	require.NoError(t, err)
	var resolutions atomic.Int32
	config := refreshingSinkParams{ChSinkShardParams: params, resolve: func() ([]*chconn.Host, error) {
		resolutions.Add(1)
		return nil, xerrors.New("unexpected refresh after fatal error")
	}}
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	mock.ExpectQuery("select count").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery("select name, type from system.columns").WillReturnRows(sqlmock.NewRows([]string{"name", "type"}).AddRow("id", "Int64"))
	mock.ExpectBegin()
	mock.ExpectPrepare("INSERT INTO")
	mock.ExpectExec("INSERT INTO").WithArgs(int64(1)).WillReturnError(&clickhouse_go.Exception{Code: 62, Message: "syntax error"})
	mock.ExpectRollback()
	mock.ExpectClose()
	server := NewSinkServerImpl(params.MakeChildServerParams(host), db, semver.Version{Major: 25}, time.UTC, logger.Log, stats.NewChStats(emptyRegistry()), nil)
	defer server.Close()
	shard := &sinkShard{
		config: config, cluster: &sinkCluster{config: config, pool: &serverPool{logger: logger.Log, servers: []*SinkServer{server}}},
		logger: logger.Log, metrics: stats.NewSinkerStats(emptyRegistry()),
		retryFunc: func(f func() error) error {
			return backoff.Retry(f, backoff.WithMaxRetries(backoff.NewConstantBackOff(0), 2))
		},
	}
	cols := abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "id", DataType: "int64", Required: true}})

	err = shard.Push([]abstract.ChangeItem{{Kind: abstract.InsertKind, Table: "test_table", ColumnNames: []string{"id"}, ColumnValues: []any{int64(1)}, TableSchema: cols}})

	require.Error(t, err)
	require.True(t, abstract.IsFatal(err))
	require.Zero(t, resolutions.Load())
	require.NoError(t, server.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}
