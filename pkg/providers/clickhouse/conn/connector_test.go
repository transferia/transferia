package conn

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
)

// database/sql passes the operation's context only to Connector.Connect. The
// callback must receive that same context while discovery is still blocked.
func TestSQLPingDeadlineInterruptsHostResolver(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	calls := 0
	db, err := ConnectNativeWithResolver(stubParams{}, func(ctx context.Context) ([]*conn_clickhouse.Host, error) {
		calls++
		if calls == 1 {
			return []*conn_clickhouse.Host{{Name: "127.0.0.1", NativePort: 1}}, nil
		}
		close(started)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return nil, errors.New("resolver released during cleanup")
		}
	})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	finished := make(chan error, 1)
	done := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		close(release)
		select {
		case <-done:
			require.NoError(t, db.Close())
		case <-time.After(time.Second):
			t.Error("SQL connection did not finish after resolver cleanup")
		}
	})
	go func() {
		defer close(done)
		finished <- db.PingContext(ctx)
	}()
	select {
	case <-started:
	case err := <-finished:
		// Both channels can be ready if this goroutine was paused beyond 20ms.
		select {
		case <-started:
			require.ErrorIs(t, err, context.DeadlineExceeded)
			return
		default:
			t.Fatalf("SQL call finished before host resolution started: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("SQL connection did not start host resolution")
	}
	select {
	case err := <-finished:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("SQL cancellation was not passed into host discovery")
	}
}

// Cancel inside credential resolution, after host discovery has already finished.
type cancelingConnParams struct {
	stubParams
	cancel context.CancelFunc
}

var _ ConnParams = cancelingConnParams{}

func (c cancelingConnParams) ResolvePassword() (string, error) {
	c.cancel()
	return "password", nil
}

// Discovery and credential resolution can finish after cancellation. Each stage
// must stop the connector before it opens a socket with the returned configuration.
func TestResolvingConnectorStopsBeforeDialingWhenContextIsCanceled(t *testing.T) {
	t.Run("before resolution", func(t *testing.T) {
		// A request already canceled must not even call discovery.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		connector := &resolvingConnector{resolveHosts: func(context.Context) ([]*conn_clickhouse.Host, error) {
			t.Fatal("canceled connection must not resolve hosts")
			return nil, nil
		}}
		_, err := connector.Connect(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})
	t.Run("during resolution", func(t *testing.T) {
		// Usable-looking addresses returned after cancellation must not be dialed.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		connector := &resolvingConnector{config: stubParams{}, resolveHosts: func(context.Context) ([]*conn_clickhouse.Host, error) {
			cancel()
			return []*conn_clickhouse.Host{{Name: "unreachable", NativePort: 9000}}, nil
		}}
		_, err := connector.Connect(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})
	t.Run("during password resolution", func(t *testing.T) {
		// Cancellation while fetching credentials must win over the socket error
		// that would otherwise come from the intentionally unreachable local port.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		connector := &resolvingConnector{config: cancelingConnParams{cancel: cancel}, resolveHosts: func(context.Context) ([]*conn_clickhouse.Host, error) {
			return []*conn_clickhouse.Host{{Name: "127.0.0.1", NativePort: 1}}, nil
		}}
		_, err := connector.Connect(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})
}

// Every physical connection needs a usable discovery result. An API error and
// an empty response must both stop connection creation, without dialing a fallback.
func TestResolvingConnectorRejectsFailedOrEmptyDiscovery(t *testing.T) {
	wantErr := errors.New("MDB unavailable")
	connector := &resolvingConnector{config: stubParams{}, resolveHosts: func(context.Context) ([]*conn_clickhouse.Host, error) {
		return nil, wantErr
	}}
	_, err := connector.Connect(context.Background())
	require.ErrorIs(t, err, wantErr)

	connector.resolveHosts = func(context.Context) ([]*conn_clickhouse.Host, error) { return nil, nil }
	_, err = connector.Connect(context.Background())
	require.ErrorContains(t, err, "no ClickHouse hosts available")
}

// Switching from the vendor's OpenDB helper to sql.OpenDB once removed the
// storage connection cap. Pool configuration is observable without opening a socket.
func TestConnectNativeWithResolverPreservesMaxOpenConnections(t *testing.T) {
	db, err := ConnectNativeWithResolver(stubParams{}, func(context.Context) ([]*conn_clickhouse.Host, error) {
		return []*conn_clickhouse.Host{{Name: "127.0.0.1", NativePort: 9000}}, nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.Equal(t, 10, db.Stats().MaxOpenConnections)
}
