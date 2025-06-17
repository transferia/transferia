//go:build !disable_greenplum_provider

package greenplum

import (
	"context"
	"io"
	"sync"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	pgsink "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type pgSinkWithPgStorage struct {
	sink abstract.Sinker
	pgs  *pgsink.Storage
}

type sinkConstructionOpts struct {
	Lgr        log.Logger
	TransferID string
	Mtrcs      metrics.Registry
}

type pgSinks interface {
	io.Closer
	PGSink(ctx context.Context, sp GPSegPointer, sinkParams PgSinkParamsRegulated) (abstract.Sinker, error)
	TotalSegments(ctx context.Context) (int, error)
	PGStorage(ctx context.Context, sp GPSegPointer) (*pgsink.Storage, error)
}

type pgSinksImpl struct {
	sinks               map[GPSegPointer]pgSinkWithPgStorage
	storage             *Storage
	opts                sinkConstructionOpts
	totalSegmentsCached int
	mutex               sync.Mutex
}

func newPgSinks(gps *Storage, lgr log.Logger, transferID string, mtrcs metrics.Registry) *pgSinksImpl {
	return &pgSinksImpl{
		sinks:   make(map[GPSegPointer]pgSinkWithPgStorage),
		storage: gps,
		opts: sinkConstructionOpts{
			Lgr:        lgr,
			TransferID: transferID,
			Mtrcs:      mtrcs,
		},
		totalSegmentsCached: 0,
		mutex:               sync.Mutex{},
	}
}

func (s *pgSinksImpl) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	errors := util.NewErrs()

	for _, s := range s.sinks {
		errors = util.AppendErr(errors, s.sink.Close())
	}
	s.storage.Close()

	if len(errors) > 0 {
		return errors
	}
	return nil
}

// PGStorage returns a PG Storage for the given segment. The resulting object MUST NOT be closed: it will be closed automatically when the sink itself is closed.
func (s *pgSinksImpl) PGStorage(ctx context.Context, sp GPSegPointer) (*pgsink.Storage, error) {
	result, err := s.storage.PGStorage(ctx, sp)
	if err != nil {
		return nil, xerrors.Errorf("failed to connect to Greenplum: %w", err)
	}
	return result, nil
}

func (s *pgSinksImpl) PGSink(ctx context.Context, sp GPSegPointer, sinkParams PgSinkParamsRegulated) (abstract.Sinker, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	actualStorage, err := s.PGStorage(ctx, sp)
	if err != nil {
		return nil, xerrors.Errorf("failed to create a PG Storage object: %w", err)
	}

	if oldSWS, ok := s.sinks[sp]; ok {
		if oldSWS.pgs == actualStorage {
			return oldSWS.sink, nil
		}
		if err := oldSWS.sink.Close(); err != nil {
			return nil, err
		}
	}

	updatePGSPRegulatedForPGStorage(&sinkParams, actualStorage)
	resultingSink, err := pgsink.NewSinkWithPool(ctx, s.opts.Lgr, s.opts.TransferID, sinkParams, s.opts.Mtrcs, actualStorage.Conn)
	if err != nil {
		return nil, xerrors.Errorf("failed to create PostgreSQL sink object: %w", err)
	}

	s.sinks[sp] = pgSinkWithPgStorage{
		sink: resultingSink,
		pgs:  actualStorage,
	}

	return resultingSink, nil
}

func (s *pgSinksImpl) TotalSegments(ctx context.Context) (int, error) {
	if s.totalSegmentsCached <= 0 {
		result, err := s.storage.TotalSegments(ctx)
		if err != nil {
			return 0, xerrors.Errorf("failed to get the total number of segments in the Greenplum cluster: %w", err)
		}
		s.totalSegmentsCached = result
	}
	return s.totalSegmentsCached, nil
}

func updatePGSPRegulatedForPGStorage(params *PgSinkParamsRegulated, pgs *pgsink.Storage) {
	params.FAllHosts = pgs.Config.AllHosts
	params.FPort = pgs.Config.Port
}
