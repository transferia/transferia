package clickhouse

import (
	stderrors "errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	defaultReviveInterval = 1 * time.Minute
)

// serverPool manages SinkServers. It round-robins over alives and periodically tries to reconnect to dead ones.
type serverPool struct {
	newServer func(*conn_clickhouse.Host) (*SinkServer, error)
	logger    log.Logger
	counter   atomic.Uint64
	mu        sync.RWMutex
	servers   []*SinkServer
	closeCh   chan struct{}
	closeOnce sync.Once
}

// newServerPool connects to every host, keeps the reachable ones as live servers and launches a background goroutine
// that keeps retrying the unreachable ones. It fails only if no reachable hosts found.
func newServerPool(hosts []*conn_clickhouse.Host, newServer func(*conn_clickhouse.Host) (*SinkServer, error), reviveInterval time.Duration, logger log.Logger) (*serverPool, error) {
	p := &serverPool{
		newServer: newServer,
		logger:    logger,
		counter:   atomic.Uint64{},
		mu:        sync.RWMutex{},
		servers:   nil,
		closeCh:   make(chan struct{}),
		closeOnce: sync.Once{},
	}

	dead, err := p.init(hosts)
	if err != nil {
		return nil, err
	}

	go p.reviveDeadHosts(dead, reviveInterval)

	return p, nil
}

// init connects to all hosts and returns the unreached ones. It is okay to retry connection to them later.
func (p *serverPool) init(hosts []*conn_clickhouse.Host) ([]*conn_clickhouse.Host, error) {
	var errs []error
	live := make([]*SinkServer, 0)
	dead := make([]*conn_clickhouse.Host, 0)
	for _, host := range yslices.Shuffle(hosts, nil) {
		p.logger.Debugf("init sinkServer %v", host.String())
		sinkServer, err := p.tryConnect(host)
		if err != nil {
			p.logger.Warn("unable to init sink server, skip", log.Error(err))
			errs = append(errs, err)
			dead = append(dead, host)
			continue
		}

		live = append(live, sinkServer)
	}
	if len(live) == 0 {
		return nil, xerrors.Errorf("no sink servers in cluster with %v hosts: %w", len(hosts), stderrors.Join(errs...))
	}
	p.servers = live
	return dead, nil
}

func (p *serverPool) tryConnect(host *conn_clickhouse.Host) (*SinkServer, error) {
	p.logger.Debugf("attempting to connect to sinkServer %v", host.String())
	sinkServer, err := p.newServer(host)
	if err != nil {
		return nil, xerrors.Errorf("unable to connect to sink server %v: %w", host.String(), err)
	}

	return sinkServer, nil
}

// best returns an alive server, round-robining over the alive ones.
func (p *serverPool) best() *SinkServer {
	p.mu.RLock()
	defer p.mu.RUnlock()

	alive := make([]*SinkServer, 0, len(p.servers))
	for _, sinkServer := range p.servers {
		if sinkServer.Alive() {
			alive = append(alive, sinkServer)
		}
	}
	if len(alive) == 0 {
		return p.servers[0]
	}

	cnt := p.counter.Add(1)
	i := int(cnt-1) % len(alive)
	p.logger.Infof("choose sinkServer %v (%v from %v counter %v)", alive[i].host, i, len(alive), cnt)

	return alive[i]
}

func (p *serverPool) serversCopy() []*SinkServer {
	p.mu.RLock()
	defer p.mu.RUnlock()

	servers := make([]*SinkServer, len(p.servers))
	copy(servers, p.servers)
	return servers
}

func (p *serverPool) close() error {
	p.closeOnce.Do(func() {
		close(p.closeCh)
	})

	p.mu.Lock()
	defer p.mu.Unlock()

	var closeErrs []error
	for _, s := range p.servers {
		if err := s.Close(); err != nil {
			closeErrs = append(closeErrs, xerrors.Errorf("failed to close SinkServer: %w", err))
		}
	}
	return stderrors.Join(closeErrs...)
}

func (p *serverPool) reviveDeadHosts(hosts []*conn_clickhouse.Host, reviveInterval time.Duration) {
	ticker := time.NewTicker(reviveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.closeCh:
			p.logger.Debug("stopping dead host revival goroutine")
			return

		case <-ticker.C:
			revivedServers, stillDead := p.attemptReviveDeadHosts(hosts)
			hosts = stillDead

			p.mu.Lock()
			select {
			case <-p.closeCh: // Double check, since after `attemptReviveDeadHosts` closeCh can be already closed.
				p.mu.Unlock()
				p.closeServers(revivedServers)
				p.logger.Debug("stopping dead host revival goroutine after attempt")
				return
			default:
			}
			if len(revivedServers) > 0 {
				p.servers = append(p.servers, revivedServers...)
			}
			p.mu.Unlock()
		}
	}
}

func (p *serverPool) attemptReviveDeadHosts(deadHosts []*conn_clickhouse.Host) ([]*SinkServer, []*conn_clickhouse.Host) {
	p.logger.Debugf("attempting to revive %d dead hosts", len(deadHosts))

	var revivedServers []*SinkServer
	var stillDead []*conn_clickhouse.Host
	for _, host := range deadHosts {
		select {
		case <-p.closeCh:
			p.logger.Debug("stopping dead host revival during reconnect")
			return revivedServers, nil // Return revivedServers so they will be properly closed.
		default:
		}

		sinkServer, err := p.tryConnect(host)
		if err != nil {
			p.logger.Warn("failed to revive host, will retry later", log.Error(err), log.String("host", host.String()))
			stillDead = append(stillDead, host)
			continue
		}

		revivedServers = append(revivedServers, sinkServer)
		p.logger.Info("successfully revived dead host", log.String("host", host.String()))
	}

	return revivedServers, stillDead
}

func (p *serverPool) closeServers(servers []*SinkServer) {
	for _, s := range servers {
		if err := s.Close(); err != nil {
			p.logger.Warn("failed to close SinkServer", log.Error(err))
		}
	}
}
