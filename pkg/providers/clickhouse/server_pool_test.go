package clickhouse

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
)

func poolHost(name string) *conn_clickhouse.Host {
	return &conn_clickhouse.Host{Name: name}
}

func stubServer(host string, alive bool) *SinkServer {
	return &SinkServer{
		host:    host,
		alive:   alive,
		logger:  logger.Log,
		closeCh: make(chan struct{}),
	}
}

func closableServer(host string, alive bool) *SinkServer {
	s := stubServer(host, alive)
	db, _, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	s.db = db
	return s
}

func serverHosts(servers []*SinkServer) []string {
	hosts := make([]string, 0, len(servers))
	for _, s := range servers {
		hosts = append(hosts, s.host)
	}
	return hosts
}

// failingFactory returns a factory that fails for hosts listed in fail and records how many connection attempts were made.
func failingFactory(fail map[string]bool, attempts *int32) func(*conn_clickhouse.Host) (*SinkServer, error) {
	return func(h *conn_clickhouse.Host) (*SinkServer, error) {
		if attempts != nil {
			atomic.AddInt32(attempts, 1)
		}
		if fail[h.Name] {
			return nil, xerrors.New("connect failed")
		}
		return closableServer(h.Name, true), nil
	}
}

func TestServerPool_InitSkipsUnreachableHosts(t *testing.T) {
	hosts := []*conn_clickhouse.Host{poolHost("a"), poolHost("b"), poolHost("c")}
	p, err := newServerPool(hosts, failingFactory(map[string]bool{"b": true}, nil), time.Millisecond, logger.Log)
	require.NoError(t, err)
	defer func() { _ = p.close() }()

	require.ElementsMatch(t, []string{"a", "c"}, serverHosts(p.serversCopy()))
}

func TestServerPool_InitFailsWhenNoHostReachable(t *testing.T) {
	hosts := []*conn_clickhouse.Host{poolHost("a"), poolHost("b")}
	p, err := newServerPool(hosts, failingFactory(map[string]bool{"a": true, "b": true}, nil), time.Millisecond, logger.Log)
	require.Error(t, err)
	require.Nil(t, p)
}

func TestServerPool_BestRoundRobinsOverAliveOnly(t *testing.T) {
	p := &serverPool{
		logger:  logger.Log,
		closeCh: make(chan struct{}),
		servers: []*SinkServer{stubServer("a", true), stubServer("b", false), stubServer("c", true)},
	}

	got := map[string]int{}
	for i := 0; i < 6; i++ {
		got[p.best().host]++
	}

	require.Zero(t, got["b"], "dead server must never be chosen")
	require.Greater(t, got["a"], 0)
	require.Greater(t, got["c"], 0)
	require.Equal(t, 6, got["a"]+got["c"])
}

func TestServerPool_BestFallsBackToFirstWhenAllDead(t *testing.T) {
	p := &serverPool{
		logger:  logger.Log,
		closeCh: make(chan struct{}),
		servers: []*SinkServer{stubServer("a", false), stubServer("b", false)},
	}

	require.Equal(t, "a", p.best().host)
}

func TestServerPool_AttemptRevivePartiallyReconnects(t *testing.T) {
	p := &serverPool{
		logger:    logger.Log,
		closeCh:   make(chan struct{}),
		newServer: failingFactory(map[string]bool{"b": true}, nil),
	}

	revived, stillDead := p.attemptReviveDeadHosts([]*conn_clickhouse.Host{poolHost("a"), poolHost("b"), poolHost("c")})

	require.ElementsMatch(t, []string{"a", "c"}, serverHosts(revived))
	require.Len(t, stillDead, 1)
	require.Equal(t, "b", stillDead[0].Name)
}

func TestServerPool_AttemptReviveStopsWhenClosed(t *testing.T) {
	var attempts int32
	p := &serverPool{
		logger:    logger.Log,
		closeCh:   make(chan struct{}),
		newServer: failingFactory(nil, &attempts),
	}
	close(p.closeCh) // pool already closed before revival runs

	revived, stillDead := p.attemptReviveDeadHosts([]*conn_clickhouse.Host{poolHost("a"), poolHost("b")})

	require.Empty(t, revived)
	require.Nil(t, stillDead)
	require.Zero(t, atomic.LoadInt32(&attempts), "must not open connections after close")
}

func TestServerPool_CloseIsIdempotent(t *testing.T) {
	p := &serverPool{
		logger:  logger.Log,
		closeCh: make(chan struct{}),
		servers: []*SinkServer{closableServer("a", true), closableServer("b", true)},
	}

	require.NoError(t, p.close())
	require.NotPanics(t, func() { _ = p.close() }, "second close must not close the channel twice")
}

func TestServerPool_ReviveReconnectsDeadHost(t *testing.T) {
	var bAttempts int32
	factory := func(h *conn_clickhouse.Host) (*SinkServer, error) {
		// "b" is unreachable on the first attempt and comes back afterwards.
		if h.Name == "b" && atomic.AddInt32(&bAttempts, 1) == 1 {
			return nil, xerrors.New("temporary")
		}
		return closableServer(h.Name, true), nil
	}

	p := &serverPool{
		logger:    logger.Log,
		newServer: factory,
		closeCh:   make(chan struct{}),
		servers:   []*SinkServer{closableServer("a", true)},
	}
	go p.reviveDeadHosts([]*conn_clickhouse.Host{poolHost("b")}, time.Millisecond)
	defer func() { _ = p.close() }()

	require.Eventually(t, func() bool {
		return len(p.serversCopy()) == 2
	}, 2*time.Second, 2*time.Millisecond)
	require.ElementsMatch(t, []string{"a", "b"}, serverHosts(p.serversCopy()))
}

func TestSinkServerClose_Idempotent(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	mock.ExpectClose()

	s := &SinkServer{db: db, logger: logger.Log, closeCh: make(chan struct{})}

	require.NoError(t, s.Close())
	require.NotPanics(t, func() { _ = s.Close() })
	require.NoError(t, mock.ExpectationsWereMet())
}
