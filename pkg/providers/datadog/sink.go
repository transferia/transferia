//go:build cgo
// +build cgo

package datadog

import (
	"context"
	"fmt"
	"strings"
	text_template "text/template"
	"time"

	vanilla_datadog "github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/cenkalti/backoff/v4"
	"github.com/spf13/cast"
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
)

type Sink struct {
	cfg      *DatadogDestination
	logger   log.Logger
	registry core_metrics.Registry
	api      *datadogV2.LogsApi
	cancel   context.CancelFunc
	ctx      context.Context
	metrics  *stats.SinkerStats
	tmpl     *text_template.Template
}

var (
	FatalErrors = set.New("403 Forbidden")
)

func (s *Sink) Close() error {
	s.cancel()
	return nil
}

func (s *Sink) Push(items []abstract.ChangeItem) error {
	cctx, cancel := context.WithTimeout(s.ctx, time.Minute)
	defer cancel()
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
				_, _, err := s.api.SubmitLog(cctx, s.mapChanges(table, chunk))
				if err != nil {
					if dgerr, ok := err.(vanilla_datadog.GenericOpenAPIError); ok && FatalErrors.Contains(dgerr.ErrorMessage) {
						return backoff.Permanent(
							abstract.NewFatalError(
								xerrors.Errorf("fatal error: %s\ndetails: %s", dgerr.ErrorMessage, string(dgerr.ErrorBody)),
							),
						)
					}
					return xerrors.Errorf("unable to submit logs: %w", err)
				}
				return nil
			}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)); err != nil {
				return err
			}
			s.metrics.Table(table.Fqtn(), "rows", len(batch[i:end]))
		}
	}
	return nil
}

func (s *Sink) mapChanges(table abstract.TableID, chunk []abstract.ChangeItem) []datadogV2.HTTPLogItem {
	return yslices.Map(chunk, func(t abstract.ChangeItem) datadogV2.HTTPLogItem {
		tmap := t.AsMap()
		messageBldr := new(strings.Builder)
		_ = s.tmpl.Execute(messageBldr, tmap)
		var tagVals []string
		for _, tag := range s.cfg.TagColumns {
			v, ok := tmap[tag]
			if !ok {
				continue
			}
			switch vv := v.(type) {
			case map[string]any:
				for k, v := range vv {
					tagVals = append(tagVals, fmt.Sprintf("%s_%s:%s", tag, k, cast.ToString(v)))
				}
			default:
				tagVals = append(tagVals, fmt.Sprintf("%s:%s", tag, cast.ToString(v)))
			}
		}
		var service *string
		var host *string
		if v, ok := tmap[s.cfg.HostColumn]; ok {
			host = vanilla_datadog.PtrString(cast.ToString(v))
		}
		if v, ok := tmap[s.cfg.ServiceColumn]; ok {
			service = vanilla_datadog.PtrString(cast.ToString(v))
		}

		return datadogV2.HTTPLogItem{
			Ddsource:             vanilla_datadog.PtrString(table.Fqtn()),
			Ddtags:               vanilla_datadog.PtrString(strings.Join(tagVals, ",")),
			Hostname:             host,
			Message:              messageBldr.String(),
			Service:              service,
			UnparsedObject:       nil,
			AdditionalProperties: nil,
		}
	})
}

func newConfiguration(cfg *DatadogDestination) *vanilla_datadog.Configuration {
	configuration := vanilla_datadog.NewConfiguration()
	allowedHosts := set.New(configuration.OperationServers["v2.LogsApi.SubmitLog"][0].Variables["site"].EnumValues...)
	if !allowedHosts.Contains(cfg.DatadogHost) {
		// default configuration for logs must be adjusted to allow current datadog host
		// driver inside itself make a check that provided datadog host contains in allowed enum-values
		configuration.OperationServers["v2.LogsApi.SubmitLog"][0] = vanilla_datadog.ServerConfiguration{
			URL:         "https://{site}",
			Description: "No description provided",
			Variables: map[string]vanilla_datadog.ServerVariable{
				"site": {DefaultValue: cfg.DatadogHost, EnumValues: []string{cfg.DatadogHost}},
			},
		}
	}
	configuration.UserAgent = "DoubleCloud/Transfer"
	return configuration
}

func NewSink(cfg *DatadogDestination, logger log.Logger, registry core_metrics.Registry) (abstract.Sinker, error) {
	tmpl, err := text_template.New("log").Parse(cfg.MessageTemplate)
	if err != nil {
		return nil, xerrors.Errorf("unable to compile log template: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ctx = context.WithValue(
		ctx,
		vanilla_datadog.ContextAPIKeys,
		map[string]vanilla_datadog.APIKey{
			"apiKeyAuth": {Key: string(cfg.ClientAPIKey)},
		},
	)
	ctx = context.WithValue(
		ctx,
		vanilla_datadog.ContextServerVariables,
		map[string]string{
			"site": cfg.DatadogHost,
		},
	)
	return &Sink{
		cfg:      cfg,
		logger:   logger,
		registry: registry,
		api:      datadogV2.NewLogsApi(vanilla_datadog.NewAPIClient(newConfiguration(cfg))),
		ctx:      ctx,
		cancel:   cancel,
		tmpl:     tmpl,
		metrics:  stats.NewSinkerStats(registry),
	}, nil
}
