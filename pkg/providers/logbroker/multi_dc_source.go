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
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

type multiDcSource struct {
	sources map[string]abstract.Source
	configs map[string]LfSource
	stats   map[string]*stats.SourceStats
	errCh   chan error
	logger  log.Logger
	metrics core_metrics.Registry
	closeCh chan struct{}
	lock    sync.Mutex
	cfg     *LfSource
}

func isPersqueueTemporaryError(err error) bool {
	persqueueError := new(persqueue.Error)
	if !xerrors.As(err, &persqueueError) {
		return false
	}
	return persqueueError.Temporary()
}

func (s *multiDcSource) Run(sink abstract.AsyncSink) error {
	endpoints, knownCluster := ClusterInstances(s.cfg.Cluster)
	if !knownCluster {
		return xerrors.Errorf("cannot run source: unknown cluster %v", s.cfg.Cluster)
	}
	endpointsNumber := len(endpoints)
	errCh := make(chan error, endpointsNumber)
	forceStop := false
	for _, endpoint := range endpoints {
		go func(endpoint LogbrokerInstance) {
			childCfg := *s.cfg
			childCfg.MaxIdleTime = time.Hour
			childCfg.Instance = endpoint
			if database, ok := clusterDefaultDatabase(s.cfg.Cluster); ok && s.cfg.Database == "" {
				childCfg.Database = database
			}
			for {
				source, err := newOneDCSource(
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
				if forceStop {
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
	err := <-errCh

	s.lock.Lock()
	forceStop = true
	s.lock.Unlock()

	s.logger.Infof("one of endpoint sources stopped (error: %v), so we need to stop other sources", err)
	for e, src := range s.sources {
		s.logger.Infof("stop endpoint source: %v", e)
		src.Stop()
	}

	s.logger.Infof("waiting for all sources are stopped")
	for i := 0; i < endpointsNumber-1; i++ {
		otherErr := <-errCh
		s.logger.Infof("endpoint source is stopped with error: %v", otherErr)
	}
	return err
}

func (s *multiDcSource) Stop() {
	for _, endpoint := range s.sources {
		endpoint.Stop()
	}
	close(s.errCh)
}

func (s *multiDcSource) Fetch() ([]abstract.ChangeItem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	res := make(chan []abstract.ChangeItem, len(s.sources))
	errCh := make(chan error, len(s.sources))
	go func() {
		endpoints, _ := ClusterInstances(s.cfg.Cluster)
		for _, endpoint := range endpoints {
			childCfg := *s.cfg
			childCfg.MaxIdleTime = time.Hour
			childCfg.Instance = endpoint
			if database, ok := clusterDefaultDatabase(s.cfg.Cluster); ok && s.cfg.Database == "" {
				childCfg.Database = database
			}
			source, err := newOneDCSource(
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

func NewMultiDCSource(cfg *LfSource, logger log.Logger, registry core_metrics.Registry) (abstract.Source, error) {
	sources := map[string]abstract.Source{}
	configs := map[string]LfSource{}
	statsM := map[string]*stats.SourceStats{}
	return &multiDcSource{
		sources: sources,
		configs: configs,
		stats:   statsM,
		errCh:   make(chan error),
		logger:  logger,
		metrics: registry,
		closeCh: make(chan struct{}),
		lock:    sync.Mutex{},
		cfg:     cfg,
	}, nil
}
