//go:build !disable_coralogix_provider

package coralogix

import (
	"context"
	"strings"
	"text/template"
	"time"

	"github.com/araddon/dateparse"
	"github.com/cenkalti/backoff/v4"
	"github.com/spf13/cast"
	"github.com/transferia/transferia/library/go/core/metrics"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/xerrors"
)

type Sink struct {
	cfg      *CoralogixDestination
	logger   log.Logger
	registry metrics.Registry
	cancel   context.CancelFunc
	ctx      context.Context
	metrics  *stats.SinkerStats
	tmpl     *template.Template
}

var FatalErrors = set.New("403 Forbidden")

func (s *Sink) Close() error {
	s.cancel()
	return nil
}

func (s *Sink) Push(items []abstract.ChangeItem) error {
	tableBatches := map[abstract.TableID][]abstract.ChangeItem{}
	for _, change := range items {
		if change.Kind != abstract.InsertKind {
			// only insert type is supported
			s.logger.Warnf("unsupported change kind: %s for table: %s", change.Kind, change.TableID().String())
			continue
		}
		tableBatches[change.TableID()] = append(tableBatches[change.TableID()], change)
	}
	for table, batch := range tableBatches {
		for i := 0; i < len(batch); i += s.cfg.ChunkSize {
			end := i + s.cfg.ChunkSize

			if end > len(batch) {
				end = len(batch)
			}

			s.metrics.Inflight.Inc()
			if err := backoff.Retry(func() error {
				chunk := batch[i:end]
				logItems := s.mapChanges(chunk)
				err := SubmitLogs(logItems, s.cfg.Domain, string(s.cfg.Token))
				if err != nil {
					if abstract.IsFatal(err) {
						return backoff.Permanent(err)
					}
					return xerrors.Errorf("unable to submit logs: %w", err)
				}
				return nil
			}, backoff.NewExponentialBackOff()); err != nil {
				return abstract.NewFatalError(xerrors.Errorf("failed to submit logs, retry exceeded: %w", err))
			}
			s.metrics.Table(table.Fqtn(), "rows", len(batch[i:end]))
		}
	}
	return nil
}

func (s *Sink) mapChanges(chunk []abstract.ChangeItem) []HTTPLogItem {
	return yslices.Map(chunk, func(t abstract.ChangeItem) HTTPLogItem {
		tmap := t.AsMap()
		messageBldr := new(strings.Builder)
		_ = s.tmpl.Execute(messageBldr, tmap)
		ts, err := dateparse.ParseAny(cast.ToString(tmap[s.cfg.TimestampColumn]))
		if err != nil {
			ts = time.Unix(int64(t.CommitTime/uint64(time.Second)), int64(t.CommitTime%uint64(time.Second)))
		}

		return HTTPLogItem{
			ApplicationName: s.cfg.ApplicationName,
			SubsystemName:   cast.ToString(tmap[s.cfg.SubsystemColumn]),
			ComputerName:    cast.ToString(tmap[s.cfg.HostColumn]),
			Timestamp:       ts.Unix(),
			Severity:        s.inferSeverity(cast.ToString(tmap[s.cfg.SeverityColumn])),
			Text:            messageBldr.String(),
			Category:        cast.ToString(tmap[s.cfg.CategoryColumn]),
			ClassName:       cast.ToString(tmap[s.cfg.ClassColumn]),
			MethodName:      cast.ToString(tmap[s.cfg.MethodColumn]),
			ThreadID:        cast.ToString(tmap[s.cfg.ThreadIDColumn]),
			HiResTimestamp:  cast.ToString(ts.UnixNano()),
		}
	})
}

func (s *Sink) inferSeverity(severity string) Severity {
	res, ok := s.cfg.KnownSevereties[severity]
	if !ok {
		return Info
	}
	return res
}

func NewSink(cfg *CoralogixDestination, logger log.Logger, registry metrics.Registry) (abstract.Sinker, error) {
	tmpl, err := template.New("log").Parse(cfg.MessageTemplate)
	if err != nil {
		return nil, xerrors.Errorf("unable to compile log template: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Sink{
		cfg:      cfg,
		logger:   logger,
		registry: registry,
		ctx:      ctx,
		cancel:   cancel,
		tmpl:     tmpl,
		metrics:  stats.NewSinkerStats(registry),
	}, nil
}
