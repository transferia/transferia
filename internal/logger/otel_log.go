package logger

import (
	"context"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.opentelemetry.io/contrib/bridges/otelzap"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.uber.org/zap"
	"go.ytsaurus.tech/library/go/core/log"
	corezap "go.ytsaurus.tech/library/go/core/log/zap"
)

const (
	KeyApp = "labels.app"
)

var OtelLog log.Logger = &corezap.Logger{L: zap.NewNop()}

type OtelLoggerConfig struct {
	Enabled     bool   `mapstructure:"enabled"`
	URI         string `mapstructure:"uri"`
	ServiceName string `mapstructure:"service_name"`
}

func NewOtelLog(ctx context.Context, cfg OtelLoggerConfig) (lgr log.Logger, close func(ctx context.Context), err error) {
	emptyCloser := func(_ context.Context) {}
	if !cfg.Enabled {
		return OtelLog, emptyCloser, nil
	}
	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(cfg.ServiceName),
		),
	)
	if err != nil {
		return nil, emptyCloser, err
	}

	logExporter, err := otlploggrpc.New(ctx,
		otlploggrpc.WithEndpointURL(cfg.URI),
		otlploggrpc.WithInsecure())
	if err != nil {
		return nil, emptyCloser, xerrors.Errorf("failed to create trace exporter: %w", err)
	}
	logProcessor := sdklog.NewBatchProcessor(logExporter)
	logProvider := sdklog.NewLoggerProvider(
		sdklog.WithResource(r),
		sdklog.WithProcessor(logProcessor),
	)
	core := otelzap.NewCore(
		"",
		otelzap.WithLoggerProvider(logProvider),
	)
	l := log.With(corezap.NewWithCore(core), log.String(KeyApp, cfg.ServiceName))
	OtelLog = l
	return l, func(ctx context.Context) {
		ctx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		if err := logProvider.Shutdown(ctx); err != nil {
			Log.Error("failed to shutdown tracer provider", log.Error(err))
		}
	}, nil
}
