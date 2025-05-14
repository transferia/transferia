package greenplum

import (
	"context"
	"sync"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/dbaas"
	"github.com/transferia/transferia/pkg/providers/postgres"
)

type mutexedPostgreses struct {
	// storages MUST NOT be accessed from outside directly. It is protected by the mutex
	storages map[GPSegPointer]*postgres.Storage
	mutex    sync.Mutex
}

func newMutexedPostgreses() mutexedPostgreses {
	return mutexedPostgreses{
		storages: make(map[GPSegPointer]*postgres.Storage),
		mutex:    sync.Mutex{},
	}
}

func (s *mutexedPostgreses) Close() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for sp, pgs := range s.storages {
		if sp.role == gpRoleCoordinator {
			continue
		}
		pgs.Close()
		delete(s.storages, sp)
	}
	if pgs, ok := s.storages[Coordinator()]; ok {
		pgs.Close()
		delete(s.storages, Coordinator())
	}
}

// PGStorage returns a live PG storage or an error
func (s *Storage) PGStorage(ctx context.Context, sp GPSegPointer) (*postgres.Storage, error) {
	s.postgreses.mutex.Lock()
	defer s.postgreses.mutex.Unlock()
	if err := s.EnsureAvailability(ctx, sp); err != nil {
		return nil, xerrors.Errorf("the requested %s is not available in the Greenplum cluster: %w", sp.String(), err)
	}
	return s.postgreses.storages[sp], nil
}

func (s *Storage) EnsureAvailability(ctx context.Context, sp GPSegPointer) error {
	if err := s.ensureCompleteClusterData(ctx); err != nil {
		return xerrors.Errorf("failed to obtain complete Greenplum cluster configuration: %w", err)
	}

	if pgs, ok := s.postgreses.storages[sp]; ok {
		err := checkConnection(ctx, pgs, sp)
		if err == nil {
			return nil
		}
		logger.Log.Warnf("an existing connection to %s (%s) has broken: %v", sp.String(), s.config.Connection.OnPremises.SegByID(sp.seg).String(), err)
		// This call leads to side effects in other goroutines that use this storage.
		// However, they should fail anyway, so that is fine.
		go pgs.Close()
		delete(s.postgreses.storages, sp)
	}

	pgs, err := s.openPGStorageForAnyInPair(ctx, sp)
	if err != nil {
		return xerrors.Errorf("failed to open PgStorage for %s (%s): %w", sp.String(), s.config.Connection.OnPremises.SegByID(sp.seg).String(), err)
	}
	s.postgreses.storages[sp] = pgs
	return nil
}

type MasterHostResolver interface {
	MasterHosts() (master string, replica string, err error)
}

func (s *Storage) ResolveDbaasMasterHosts() (master, replica *GpHP, err error) {
	instnc, err := dbaas.Current()
	if err != nil {
		return nil, nil, xerrors.Errorf("unable to build instance: %w", err)
	}
	resolver, err := instnc.HostResolver(dbaas.ProviderTypeGreenplum, s.config.Connection.MDBCluster.ClusterID)
	if err != nil {
		return nil, nil, xerrors.Errorf("unable to build resolver: %w", err)
	}
	masterResolver, ok := resolver.(MasterHostResolver)
	if !ok {
		return nil, nil, xerrors.Errorf("unknown resolver: %T", resolver)
	}
	masterHost, replicaHost, err := masterResolver.MasterHosts()
	return NewGpHP(masterHost, 6432), NewGpHP(replicaHost, 6432), err
}

func (s *Storage) ensureCompleteClusterData(ctx context.Context) error {
	if s.config.Connection.OnPremises == nil {
		master, replica, err := s.ResolveDbaasMasterHosts()
		if err != nil {
			return xerrors.Errorf("Unable to get host names: %w", err)
		}
		s.config.Connection.OnPremises = new(GpCluster)
		s.config.Connection.OnPremises.Coordinator = new(GpHAP)
		s.config.Connection.OnPremises.Coordinator.Primary = master
		s.config.Connection.OnPremises.Coordinator.Mirror = replica
	}

	if len(s.config.Connection.OnPremises.Segments) > 0 {
		return nil
	}

	pgs, err := s.openPGStorageForAnyInPair(ctx, Coordinator())
	if err != nil {
		return xerrors.Errorf("failed to open PgStorage for %s (%s): %w", Coordinator().String(), s.config.Connection.OnPremises.SegByID(Coordinator().seg).String(), err)
	}
	s.postgreses.storages[Coordinator()] = pgs

	// XXX: This method may be made fault-tolerant when the whole transfer is fault-tolerant to Greenplum coordinator failures.
	// For now, when coordinator fails, we restart the whole transfer, so this error is not a problem.
	segments, err := segmentsFromGP(ctx, s.postgreses.storages[Coordinator()])
	if err != nil {
		return xerrors.Errorf("failed to obtain a list of segments from Greenplum: %w", err)
	}
	s.config.Connection.OnPremises.Segments = segments

	return nil
}

// TotalSegments returns the actual total number of segments in Greenplum cluster. Never returns `0`
func (s *Storage) TotalSegments(ctx context.Context) (int, error) {
	s.postgreses.mutex.Lock()
	defer s.postgreses.mutex.Unlock()
	if err := s.EnsureAvailability(ctx, Coordinator()); err != nil {
		return 0, xerrors.Errorf("Greenplum is unavailable: %w", err)
	}
	if len(s.config.Connection.OnPremises.Segments) == 0 {
		return 0, abstract.NewFatalError(xerrors.New("Greenplum cluster contains 0 segments"))
	}
	return len(s.config.Connection.OnPremises.Segments), nil
}
