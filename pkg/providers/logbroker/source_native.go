package logbroker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/transferia/transferia/kikimr/public/sdk/go/persqueue"
	"github.com/transferia/transferia/kikimr/public/sdk/go/persqueue/log/corelogadapter"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/format"
	"github.com/transferia/transferia/pkg/providers/logbroker/queues"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/xtls"
	"go.ytsaurus.tech/library/go/core/log"
)

type publisher struct {
	config           *LbSource
	errCh            chan error
	consumer         persqueue.Reader
	logger           log.Logger
	metrics          *stats.SourceStats
	stopCh           chan bool
	once             sync.Once
	cancel           context.CancelFunc
	offsetsValidator *queues.LbOffsetsSourceValidator
}

type changesOrError struct {
	data         []abstract.ChangeItem
	err          error
	rawDataBytes uint64
}

func (p *publisher) Stop() {
	p.once.Do(func() {
		close(p.stopCh)
		p.cancel()
		<-p.consumer.Closed()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			p.logger.Warn("timeout in lb reader abort")
			return
		case <-p.consumer.Closed():
			p.logger.Info("abort lb reader")
			return
		}
	}
}

func (p *publisher) lockPartition(lock *persqueue.Lock) {
	partName := fmt.Sprintf("%v@%v", lock.Topic, lock.Partition)
	p.logger.Infof("Lock partition:%v ReadOffset:%v, EndOffset:%v", partName, lock.ReadOffset, lock.EndOffset)
	p.offsetsValidator.InitOffsetForPartition(lock.Topic, lock.Partition, lock.ReadOffset)
	lock.StartRead(true, lock.ReadOffset, lock.ReadOffset)
}

func (p *publisher) sendSynchronizeEventIfNeeded(sink abstract.AsyncSink) error {
	if p.config.IsLbSink && sink != nil {
		p.logger.Info("Sending synchronize event")
		err := <-sink.AsyncPush([]abstract.ChangeItem{abstract.MakeSynchronizeEvent()})
		if err != nil {
			return xerrors.Errorf("unable to reset writers in sink: %w", err)
		}
		p.logger.Info("Sent synchronize event")
	}
	return nil
}

func (p *publisher) Run(sink abstract.AsyncSink) error {
	defer func() {
		p.consumer.Shutdown()
		p.cancel()
		p.logger.Info("Start gracefully close lb reader")
		for {
			select {
			case m := <-p.consumer.C():
				switch v := m.(type) {
				case *persqueue.Data:
					var skips []map[string]interface{}
					for _, b := range v.Batches() {
						for _, msg := range b.Messages {
							p.logger.Debugf("message: %v@%v at %v", b.Topic, b.Partition, msg.Offset)
							skips = append(skips, map[string]interface{}{
								"topic":     b.Topic,
								"partition": b.Partition,
								"offset":    msg.Offset,
							})
						}
					}
					p.logger.Warn("skipped message data messages", log.Any("cookie", v.Cookie), log.Any("skips", skips))

				case *persqueue.Disconnect:
					if v.Err != nil {
						p.logger.Infof("Disconnected: %s", v.Err.Error())
					} else {
						p.logger.Info("Disconnected")
					}
				case nil:
					p.logger.Info("Semi-gracefully closed")
					return

				default:
					p.logger.Infof("Received unexpected Event type: %T", m)
				}
			case <-p.consumer.Closed():
				p.logger.Info("Gracefully closed")
				return
			}
		}
	}()

	for {
		select {
		case <-p.stopCh:
			return nil

		case err := <-p.errCh:
			p.logger.Error("consumer error", log.Error(err))
			return xerrors.Errorf("consumer error: %w", err)

		case b, ok := <-p.consumer.C():
			if !ok {
				p.logger.Warn("Reader closed")
				return errors.New("consumer closed, close subscription")
			}

			stat := p.consumer.Stat()
			p.metrics.Usage.Set(float64(stat.MemUsage))
			p.metrics.Read.Set(float64(stat.BytesRead))
			p.metrics.Extract.Set(float64(stat.BytesExtracted))

			switch v := b.(type) {
			case *persqueue.Lock:
				p.lockPartition(v)
			case *persqueue.Release:
				p.logger.Infof("Received 'Release' event, partition:%s@%d", v.Topic, v.Partition)
				err := p.sendSynchronizeEventIfNeeded(sink)
				if err != nil {
					return xerrors.Errorf("unable to send synchronize event, err: %w", err)
				}
			case *persqueue.Disconnect:
				if v.Err != nil {
					p.logger.Errorf("Disconnected: %s", v.Err.Error())
				} else {
					p.logger.Error("Disconnected")
				}
				err := p.sendSynchronizeEventIfNeeded(sink)
				if err != nil {
					return xerrors.Errorf("unable to send synchronize event, err: %w", err)
				}
			case *persqueue.Data:
				batches := queues.ConvertBatches(v.Batches())
				messagesSize, messagesCount := BatchStatistics(batches)
				p.metrics.Size.Add(messagesSize)
				p.metrics.Count.Add(messagesCount)
				err := p.offsetsValidator.CheckLbOffsets(batches)
				if err != nil {
					if p.config.AllowTTLRewind {
						p.logger.Warn("ttl rewind", log.Error(err))
					} else {
						p.metrics.Fatal.Inc()
						return abstract.NewFatalError(err)
					}
				}
				p.logger.Debug("got lb_offsets", log.Any("range", queues.BuildMapPartitionToLbOffsetsRange(batches)))

				p.metrics.Master.Set(1)
				parseQ := []chan changesOrError{}
				mapPartitionToLbOffsetsRange := queues.BuildMapPartitionToLbOffsetsRange(batches)
				for _, b := range v.Batches() {
					for _, m := range b.Messages {
						resCh := make(chan changesOrError, 1)
						parseQ = append(parseQ, resCh)
						go func(m persqueue.ReadMessage) {
							start := time.Now()
							batch, err := abstract.UnmarshalChangeItems(m.Data)
							if err != nil {
								p.logger.Warn("Unable to convert body to changeItems", log.Error(err), log.Any("body", util.Sample(string(m.Data), 1*1024)))
								p.metrics.Unparsed.Inc()
								resCh <- changesOrError{
									data:         nil,
									err:          err,
									rawDataBytes: uint64(len(m.Data)),
								}
								return
							}
							p.metrics.DecodeTime.RecordDuration(time.Since(start))
							p.metrics.ChangeItems.Add(int64(len(batch)))
							for _, ci := range batch {
								if ci.IsRowEvent() {
									p.metrics.Parsed.Inc()
								}
							}
							resCh <- changesOrError{
								data:         batch,
								err:          nil,
								rawDataBytes: uint64(len(m.Data)),
							}
						}(m)
					}
				}
				messages := make([]abstract.ChangeItem, 0)
				var parseErrors []error
				for _, batch := range parseQ {
					item := <-batch
					if item.err != nil {
						parseErrors = append(parseErrors, item.err)
					} else {
						etaRawItemSize := item.rawDataBytes / uint64(len(item.data))
						for i := range item.data {
							item.data[i].Size = abstract.RawEventSize(etaRawItemSize)
						}
						messages = append(messages, item.data...)
					}
				}
				if len(parseErrors) > 0 {
					totalErr := xerrors.New("")
					for _, err := range parseErrors {
						totalErr = xerrors.Errorf("%v\n%w", totalErr, err)
					}
					return xerrors.Errorf("parse errors:\n%w", totalErr)
				}
				startTime := time.Now()
				go func(pqd *persqueue.Data, startTime time.Time, outputCh chan error) {
					if err := <-outputCh; err != nil {
						p.sendError(err)
						return
					}
					elapsed := time.Since(startTime)
					pqd.Commit()
					p.metrics.PushTime.RecordDuration(elapsed)
					p.logger.Debug(
						"Pushed",
						log.Any("usage", format.SizeInt(p.consumer.Stat().MemUsage)),
						log.Any("count", len(v.Batches())),
						log.Any("lb_offsets", mapPartitionToLbOffsetsRange),
					)
				}(v, startTime, sink.AsyncPush(messages))
			}
		}
	}
}

func (p *publisher) sendError(err error) {
	select {
	case p.errCh <- err:
	case <-p.stopCh:
		p.logger.Error("Push error after consumer stopped", log.Error(err))
	}
}

func NewNativeSource(cfg *LbSource, logger log.Logger, registry metrics.Registry) (abstract.Source, error) {
	var opts persqueue.ReaderOptions
	opts.Logger = corelogadapter.New(logger)
	opts.Endpoint = cfg.Instance
	opts.Database = cfg.Database
	opts.ManualPartitionAssignment = true
	opts.Consumer = cfg.Consumer
	opts.Topics = []persqueue.TopicInfo{{Topic: cfg.Topic}}
	opts.MaxReadSize = 1 * 1024 * 1024
	opts.MaxMemory = 100 * 1024 * 1024 // 100 mb max memory usage
	opts.RetryOnFailure = true
	opts.Port = cfg.Port
	opts.Credentials = cfg.Credentials

	if cfg.TLS == EnabledTLS {
		tls, err := xtls.FromPath(cfg.RootCAFiles)
		if err != nil {
			return nil, xerrors.Errorf("failed to get TLS config for cloud: %w", err)
		}
		opts.TLSConfig = tls
	}

	c := persqueue.NewReader(opts)

	ctx, cancel := context.WithCancel(context.Background())
	if _, err := c.Start(ctx); err != nil {
		cancel()
		return nil, xerrors.Errorf("failed to start source reader: %w", err)
	}

	stopCh := make(chan bool)

	p := publisher{
		config:           cfg,
		errCh:            make(chan error),
		consumer:         c,
		logger:           logger,
		metrics:          stats.NewSourceStats(registry),
		stopCh:           stopCh,
		once:             sync.Once{},
		cancel:           cancel,
		offsetsValidator: queues.NewLbOffsetsSourceValidator(logger),
	}

	return &p, nil
}
