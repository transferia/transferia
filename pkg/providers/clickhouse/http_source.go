package clickhouse

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/connection/clickhouse"
	middlewares2 "github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers/clickhouse/format"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/middlewares"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	defaultIOFormat = model.ClickhouseIOFormatCSV
)

type HTTPSource struct {
	client httpclient.HTTPClient

	config  *model.ChStorageParams
	query   string
	metrics *stats.SourceStats
	hosts   []*clickhouse.Host
	part    *TablePartA2
	cols    *abstract.TableSchema

	state httpSourceState

	lgr        log.Logger
	countQuery string
}

type httpSourceState struct {
	sync.Mutex
	StopRequested bool
	Running       bool
	Current       uint64
	Total         uint64
}

func (s *HTTPSource) Running() bool {
	s.state.Lock()
	defer s.state.Unlock()
	return s.state.Running
}

func (s *HTTPSource) IOFormat() model.ClickhouseIOFormat {
	if s.config.IOHomoFormat != "" {
		return s.config.IOHomoFormat
	}
	return defaultIOFormat
}

func (s *HTTPSource) Start(ctx context.Context, target base.EventTarget) error {
	s.state.Lock()
	if s.state.StopRequested {
		s.state.Unlock()
		return nil
	}
	res, err := backoff.RetryWithData[uint64](func() (uint64, error) {
		return s.fetchCount(ctx)
	}, backoff.NewExponentialBackOff())
	if err != nil {
		s.state.Unlock()
		return xerrors.Errorf("unable to fetch %s count: %w", s.part.FullName(), err)
	}
	s.lgr.Info("Row count obtained for ClickHouse table", log.String("table", s.part.TableID.Fqtn()), log.String("query", s.countQuery), log.UInt64("len", res))
	s.state.Total = res
	s.state.Current = 0
	s.state.Running = true
	defer func() {
		s.state.Lock()
		defer s.state.Unlock()
		s.state.Running = false
	}()
	s.state.Unlock()

	syncTarget := middlewares2.OutputDataBatchMetering()(middlewares.NewEventTargetWrapper(target))
	if err := backoff.RetryNotify(
		func() error {
			return s.rowsByHTTP(ctx, syncTarget)
		},
		backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 12),
		util.BackoffLogger(s.lgr, "upload"),
	); err != nil {
		if errST := syncTarget.Close(); errST != nil {
			s.lgr.Warn("failed to Close destination after an error in events provider", log.Error(errST))
		}
		return xerrors.Errorf("failed to provide or push events from events provider: %w", err)
	}
	if err := syncTarget.Close(); err != nil {
		return xerrors.Errorf("failed to push events to destination: %w", err)
	}

	return nil
}

func (s *HTTPSource) fetchCount(ctx context.Context) (uint64, error) {
	var res uint64
	host := s.hosts[0]
	if len(s.hosts) > 1 {
		host = s.hosts[rand.Intn(len(s.hosts))]
	}
	if err := s.client.Query(ctx, s.lgr, host, s.countQuery, &res); err != nil {
		return 0, xerrors.Errorf("unable to select exact row count: %w", err)
	}
	return res, nil
}

func (s *HTTPSource) rowsByHTTP(ctx context.Context, syncTarget middlewares.Asynchronizer) error {
	st := time.Now()
	query := buildQuery(s.query, s.part.Part.Rows, s.state.Current, string(s.IOFormat()))
	s.lgr.Info("Start query in ClickHouse", log.String("table", s.part.TableID.Fqtn()), log.String("query", query))
	body, err := s.client.QueryStream(ctx, s.lgr, s.hosts[0], query)
	if err != nil {
		return xerrors.Errorf("unable to exec request: %s: %v", s.part.TableID.Fqtn(), err)
	}
	rollbacks := util.Rollbacks{}
	rollbacks.Add(func() {
		if err := body.Close(); err != nil {
			s.lgr.Warn("Failed to close ClickHouse response object", log.Error(err))
		}
	})
	defer rollbacks.Do()

	// size is 1.1 of the s.config.BufferSize parameter. It should save us from unnecessary allocations
	// at the first filling up to s.config.BufferSize + a little more
	validBuffer := bytes.NewBuffer(make([]byte, 0, s.config.BufferSize+s.config.BufferSize/10))
	teeReader := io.TeeReader(body, validBuffer)
	validator, err := format.NewValidator(teeReader, s.IOFormat(), len(s.cols.Columns()))
	if err != nil {
		return xerrors.Errorf("unable to build validator, err: %w", err)
	}
	rowsCount, bytesCount := 0, 0
	for {
		if s.isStopRequested() {
			// rollbacks will clean up
			return nil
		}
		readBytes, err := validator.ReadAndValidate()

		if err != nil {
			if xerrors.Is(err, io.EOF) {
				s.lgr.Info("stop reading cause EOF")
				break
			}
			if readBytes > 0 {
				validBuffer.Truncate(bytesCount)
				s.lgr.Warnf("got error on parsing data: %s", string(validBuffer.Bytes()[:readBytes]))
			}
			return xerrors.Errorf("failed to read or split rows: %w", err)
		}

		s.metrics.Size.Add(readBytes)
		bytesCount += int(readBytes)
		rowsCount++
		if uint64(bytesCount) > s.config.BufferSize {
			s.lgr.Infof("consumed %s, %v (%v / %v) rows from %v in %v", humanize.Bytes(uint64(bytesCount)), rowsCount, s.state.Current, s.state.Total, s.part.FullName(), time.Since(st))
			st = time.Now()
			tmp := make([]byte, bytesCount)
			if _, err := validBuffer.Read(tmp); err != nil {
				s.lgr.Info("validation buffer is empty", log.Error(err))
				break
			}
			if err := syncTarget.Push(NewHTTPEventsBatch(s.part, tmp, s.cols, util.GetTimestampFromContextOrNow(ctx), s.IOFormat(), rowsCount, bytesCount)); err != nil {
				return xerrors.Errorf("failed to push a batch of %d rows (%s) into destination: %w", rowsCount, humanize.Bytes(uint64(len(tmp))), err)
			}
			func() {
				s.state.Lock()
				defer s.state.Unlock()
				s.state.Current = s.state.Current + uint64(rowsCount)
			}()
			rowsCount, bytesCount = 0, 0
		}
	}
	if validBuffer.Len() > 0 {
		s.lgr.Infof("leftovers: %s in %v", humanize.Bytes(uint64(validBuffer.Len())), time.Since(st))
		if err := syncTarget.Push(NewHTTPEventsBatch(s.part, validBuffer.Bytes(), s.cols, util.GetTimestampFromContextOrNow(ctx), s.IOFormat(), rowsCount, bytesCount)); err != nil {
			return xerrors.Errorf("failed to push the last batch of %d rows (%s) into destination: %w", rowsCount, humanize.Bytes(uint64(validBuffer.Len())), err)
		}
	}
	func() {
		s.state.Lock()
		defer s.state.Unlock()
		s.state.Current = s.state.Current + uint64(rowsCount)
	}()
	rollbacks.Cancel() // why no Close() is required? Left as is from the previous version
	return nil
}

func (s *HTTPSource) isStopRequested() bool {
	s.state.Lock()
	defer s.state.Unlock()
	return s.state.StopRequested
}

func (s *HTTPSource) Stop() error {
	s.state.Lock()
	defer s.state.Unlock()
	s.state.StopRequested = true
	return nil
}

func (s *HTTPSource) Progress() (base.EventSourceProgress, error) {
	s.state.Lock()
	defer s.state.Unlock()
	return base.NewDefaultEventSourceProgress(!s.state.Running, s.state.Current, s.state.Total), nil
}

func NewHTTPSourceImpl(
	logger log.Logger,
	query string,
	countQuery string,
	cols *abstract.TableSchema,
	hosts []*clickhouse.Host,
	config *model.ChStorageParams,
	part *TablePartA2,
	sourceStats *stats.SourceStats,
	client httpclient.HTTPClient,
) *HTTPSource {
	return &HTTPSource{
		client: client,

		config:     config,
		query:      query,
		countQuery: countQuery,
		metrics:    sourceStats,
		hosts:      hosts,
		part:       part,
		cols:       cols,

		state: httpSourceState{
			Mutex:         sync.Mutex{},
			StopRequested: false,
			Running:       false,
			Current:       0,
			Total:         0,
		},

		lgr: logger,
	}
}

func NewHTTPSource(
	logger log.Logger,
	query string,
	countQuery string,
	cols *abstract.TableSchema,
	hosts []*clickhouse.Host,
	config *model.ChSource,
	part *TablePartA2,
	sourceStats *stats.SourceStats,
) (*HTTPSource, error) {
	storageParams, err := config.ToStorageParams()
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve storage params")
	}

	cl, err := httpclient.NewHTTPClientImpl(storageParams.ToConnParams())
	if err != nil {
		return nil, xerrors.Errorf("error creating CH HTTP client: %w", err)
	}
	return NewHTTPSourceImpl(
		logger,
		query,
		countQuery,
		cols,
		hosts,
		storageParams,
		part,
		sourceStats,
		cl,
	), nil
}
