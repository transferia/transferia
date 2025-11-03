package logbroker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/transferia/transferia/kikimr/public/sdk/go/persqueue"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

var (
	KnownClusters = map[LogbrokerCluster][]LogbrokerInstance{
		Logbroker: {
			"sas.logbroker.yandex.net",
			"vla.logbroker.yandex.net",
			"klg.logbroker.yandex.net",
		},
		LogbrokerPrestable: {
			"myt.logbroker-prestable.yandex.net",
			"vla.logbroker-prestable.yandex.net",
			"klg.logbroker-prestable.yandex.net",
			"sas.logbroker-prestable.yandex.net",
		},
		Lbkx:                 {"lbkx.logbroker.yandex.net"},
		Messenger:            {"messenger.logbroker.yandex.net"},
		Lbkxt:                {"lbkxt.logbroker.yandex.net"},
		YcLogbroker:          {"lb.etn03iai600jur7pipla.ydb.mdb.yandexcloud.net"},
		YcLogbrokerPrestable: {"lb.cc8035oc71oh9um52mv3.ydb.mdb.cloud-preprod.yandex.net"},
	}

	knownDatabases = map[LogbrokerCluster]string{
		YcLogbroker:          "/global/b1gvcqr959dbmi1jltep/etn03iai600jur7pipla",
		YcLogbrokerPrestable: "/pre-prod_global/aoeb66ftj1tbt1b2eimn/cc8035oc71oh9um52mv3",
	}
)

type multiDcSource struct {
	sources map[string]abstract.Source
	configs map[string]LfSource
	stats   map[string]*stats.SourceStats
	errCh   chan error
	logger  log.Logger
	metrics metrics.Registry
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
	endpoints, knownCluster := KnownClusters[s.cfg.Cluster]
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
			if _, ok := knownDatabases[s.cfg.Cluster]; ok && s.cfg.Database == "" {
				childCfg.Database = knownDatabases[s.cfg.Cluster]
			}
			for {
				source, err := NewOneDCSource(
					&childCfg,
					log.With(s.logger, log.String("dc", string(endpoint))),
					s.metrics.WithTags(map[string]string{"dc": string(endpoint)}),
					5,
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
		for url, endpoint := range KnownClusters[s.cfg.Cluster] {
			childCfg := *s.cfg
			childCfg.MaxIdleTime = time.Hour
			childCfg.Instance = endpoint
			if _, ok := knownDatabases[s.cfg.Cluster]; ok && s.cfg.Database == "" {
				childCfg.Database = knownDatabases[s.cfg.Cluster]
			}
			source, err := NewOneDCSource(
				&childCfg,
				s.logger,
				s.metrics.WithTags(map[string]string{"dc": string(endpoint)}),
				5,
			)
			if err != nil {
				errCh <- err
				return
			}
			s.logger.Infof("start read one of %v", url)
			if r, err := source.(abstract.Fetchable).Fetch(); err != nil {
				res <- r
			} else {
				errCh <- err
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

func NewMultiDCSource(cfg *LfSource, logger log.Logger, registry metrics.Registry) (abstract.Source, error) {
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
