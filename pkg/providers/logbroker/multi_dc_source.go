package logbroker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/transferia/transferia/kikimr/public/sdk/go/persqueue"
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

type multiDcSource struct {
	sources map[string]abstract.Source
	logger  log.Logger
	metrics core_metrics.Registry
	// ctx/cancel let Run (and Stop) interrupt endpoint goroutines that are stuck
	// trying to create a reader for an unreachable endpoint: without this, such a
	// goroutine never notices that its siblings have already failed (or that the
	// whole source is being stopped) and Run would block forever waiting for it.
	ctx       context.Context
	cancel    context.CancelFunc
	lock      sync.Mutex
	cfg       *LfSource
	stopOnce  sync.Once
	resolve   func(context.Context, *LfSource, log.Logger) ([]LogbrokerInstance, error)
	newSource func(context.Context, *LfSource, log.Logger, core_metrics.Registry) (abstract.Source, error)
}

func isPersqueueTemporaryError(err error) bool {
	persqueueError := new(persqueue.Error)
	if !xerrors.As(err, &persqueueError) {
		return false
	}
	return persqueueError.Temporary()
}

func (s *multiDcSource) Run(sink abstract.AsyncSink) error {
	endpoints, err := s.resolve(s.ctx, s.cfg, s.logger)
	if err != nil {
		return xerrors.Errorf("resolve Logbroker sources: %w", err)
	}
	endpointsNumber := len(endpoints)
	errCh := make(chan error, endpointsNumber)
	for _, endpoint := range endpoints {
		go func(endpoint LogbrokerInstance) {
			childCfg := s.configForInstance(endpoint)
			for {
				select {
				case <-s.ctx.Done():
					// Either a sibling endpoint has already failed and the whole
					// multi-dc source is being torn down, or Stop was
					// called directly. Bail out instead of attempting (or retrying)
					// a connection to this (possibly still unreachable) endpoint.
					errCh <- xerrors.Errorf("won`t init endpoint(%v) source: %w", string(endpoint), s.ctx.Err())
					return
				default:
				}

				source, err := s.newSource(
					s.ctx,
					&childCfg,
					log.With(s.logger, log.String("dc", string(endpoint))),
					s.metrics.WithTags(map[string]string{"dc": string(endpoint)}),
				)
				if err != nil {
					if abstract.IsFatal(err) {
						errCh <- err
						return
					}
					s.logger.Error(fmt.Sprintf("unable to init source for endpoint %v, retry", string(endpoint)), log.Error(err))
					continue
				}

				s.lock.Lock()
				if s.ctx.Err() != nil {
					s.lock.Unlock()
					source.Stop()
					errCh <- xerrors.Errorf("won`t run endpoint(%v) source because of forced stop", string(endpoint))
					return
				}
				s.sources[string(endpoint)] = source
				s.lock.Unlock()

				err = source.Run(sink)

				if isPersqueueTemporaryError(err) {
					s.logger.Error(fmt.Sprintf("endpoint(%v) source run failed with persqueue temporary error, retry", string(endpoint)), log.Error(err))
					continue
				}

				errCh <- err
				return
			}
		}(endpoint)
	}
	err = <-errCh

	// Cancel ctx so that any endpoint goroutine still stuck creating a reader for an
	// unreachable endpoint (and therefore not yet present in
	// s.sources) unblocks and reports into errCh below, instead of hanging forever.
	s.cancel()

	s.logger.Infof("one of endpoint sources stopped (error: %v), so we need to stop other sources", err)
	s.lock.Lock()
	for e, src := range s.sources {
		s.logger.Infof("stop endpoint source: %v", e)
		src.Stop()
	}
	s.lock.Unlock()

	s.logger.Infof("waiting for all sources are stopped")
	for i := 0; i < endpointsNumber-1; i++ {
		otherErr := <-errCh
		s.logger.Infof("endpoint source is stopped with error: %v", otherErr)
	}
	return err
}

func (s *multiDcSource) Stop() {
	// Unblock any endpoint goroutine still creating a reader for an unreachable
	// endpoint, in addition to stopping the already-started ones below.
	s.stopOnce.Do(func() {
		s.cancel()
		s.lock.Lock()
		defer s.lock.Unlock()
		for _, source := range s.sources {
			source.Stop()
		}
	})
}

func (s *multiDcSource) Fetch() ([]abstract.ChangeItem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	endpoints, err := s.resolve(ctx, s.cfg, s.logger)
	if err != nil {
		return nil, xerrors.Errorf("resolve Logbroker sources for sampling: %w", err)
	}
	res := make(chan []abstract.ChangeItem, len(s.sources))
	errCh := make(chan error, len(s.sources))
	go func() {
		for _, endpoint := range endpoints {
			childCfg := s.configForInstance(endpoint)
			source, err := s.newSource(
				ctx,
				&childCfg,
				s.logger,
				s.metrics.WithTags(map[string]string{"dc": string(endpoint)}),
			)
			if err != nil {
				errCh <- err
				return
			}
			s.logger.Infof("start read one of %v", endpoint)
			if r, err := source.(abstract.Fetchable).Fetch(); err != nil {
				errCh <- err
			} else {
				res <- r
			}
		}
	}()

	for {
		select {
		case err := <-errCh:
			return nil, err
		case r := <-res:
			if len(r) == 0 {
				s.logger.Info("skip empty result")
				continue
			}
			s.logger.Infof("sample result fetched")
			return r, nil
		case <-ctx.Done():
			return nil, xerrors.New("unable to fetch sample data, timeout reached")
		}
	}
}

func (s *multiDcSource) configForInstance(endpoint LogbrokerInstance) LfSource {
	childCfg := *s.cfg
	childCfg.MaxIdleTime = time.Hour
	childCfg.Instance = endpoint
	if database, ok := clusterDefaultDatabase(s.cfg.Cluster); ok && s.cfg.Database == "" {
		childCfg.Database = database
	}
	return childCfg
}

func NewMultiDCSource(cfg *LfSource, logger log.Logger, registry core_metrics.Registry) (abstract.Source, error) {
	sources := map[string]abstract.Source{}
	ctx, cancel := context.WithCancel(context.Background())
	return &multiDcSource{
		sources:   sources,
		logger:    logger,
		metrics:   registry,
		ctx:       ctx,
		cancel:    cancel,
		lock:      sync.Mutex{},
		stopOnce:  sync.Once{},
		cfg:       cfg,
		resolve:   resolveInstances,
		newSource: newOneDCSource,
	}, nil
}
