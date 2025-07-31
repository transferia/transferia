package logger

import (
	"context"
	"time"

	"github.com/transferia/transferia/cloud/dataplatform/appconfig"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/yandex/tvm"
	"github.com/transferia/transferia/library/go/yandex/tvm/tvmauth"
	"go.opentelemetry.io/contrib/bridges/otelzap"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.uber.org/zap"
	"go.ytsaurus.tech/library/go/core/log"
	corezap "go.ytsaurus.tech/library/go/core/log/zap"
	"google.golang.org/grpc"
)

const (
	KeyApp           = "labels.app"
	transferTVMAlias = "data-transfer"
	tvmHeader        = "X-Ya-Service-Ticket"
)

var (
	FatalErrorLog log.Logger = &corezap.Logger{L: zap.NewNop()}
	noOp          log.Logger = &corezap.Logger{L: zap.NewNop()}
	emptyCloser              = func(_ context.Context) {}
)

type OtelLogConfig struct {
	Enabled bool    `mapstructure:"enabled"`
	Config  OtelLog `mapstructure:"config"`
}

// go-sumtype:decl OtelLog
type OtelLog interface {
	appconfig.TypeTagged
	isOtelLog()
}

func init() {
	appconfig.RegisterTypeTagged((*OtelLog)(nil), (*OtelUnifiedAgent)(nil), "otel_unified_agent", nil)
	appconfig.RegisterTypeTagged((*OtelLog)(nil), (*OtelDirect)(nil), "otel_direct", nil)
}

type OtelUnifiedAgent struct {
	URI         string `mapstructure:"uri"`
	ServiceName string `mapstructure:"service_name"`
}

func (o *OtelUnifiedAgent) IsTypeTagged() {}

func (o *OtelUnifiedAgent) isOtelLog() {}

type OtelDirect struct {
	URI         string    `mapstructure:"uri"`
	ServiceName string    `mapstructure:"service_name"`
	Project     string    `mapstructure:"project"`
	TVM         TVMConfig `mapstructure:"tvm"`
}

func (o *OtelDirect) IsTypeTagged() {}

func (o *OtelDirect) isOtelLog() {}

type TVMConfig struct {
	ClientID      int              `mapstructure:"client_id"`
	DestinationID int              `mapstructure:"destination_id"`
	Secret        appconfig.Secret `mapstructure:"secret"`
}

type authInjector struct {
	tvm *tvmauth.Client
}

func newAuthInjector(tvmSelfID int, TVMLoggingID int, tvmSecret string) (*authInjector, error) {
	t, err := tvmauth.NewAPIClient(tvmauth.TvmAPISettings{
		SelfID: tvm.ClientID(tvmSelfID),
		ServiceTicketOptions: tvmauth.NewAliasesOptions(tvmSecret, map[string]tvm.ClientID{
			transferTVMAlias: tvm.ClientID(TVMLoggingID)}),
	}, Log)
	if err != nil {
		return nil, xerrors.Errorf("tvm init error: %w", err)
	}
	return &authInjector{tvm: t}, nil
}

func (a *authInjector) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	ticket, err := a.tvm.GetServiceTicketForAlias(ctx, transferTVMAlias)
	if err != nil {
		return nil, xerrors.Errorf("error getting TVM service ticket: %w", err)
	}
	return map[string]string{
		tvmHeader: ticket,
	}, nil
}

func (a *authInjector) RequireTransportSecurity() bool {
	return true
}

func NewOtelLog(ctx context.Context, cfg OtelLogConfig) (lgr log.Logger, close func(ctx context.Context), err error) {
	if !cfg.Enabled {
		return noOp, emptyCloser, nil
	}
	switch c := cfg.Config.(type) {
	case *OtelUnifiedAgent:
		return newOtelUnifiedAgent(ctx, *c)
	case *OtelDirect:
		return newDirectOtel(ctx, *c)
	default:
		return noOp, emptyCloser, xerrors.Errorf("unknown otel log type: %T", cfg)
	}
}

func newDirectOtel(ctx context.Context, cfg OtelDirect) (log.Logger, func(ctx context.Context), error) {
	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(cfg.ServiceName),
			attribute.String("project", cfg.Project),
		),
	)
	if err != nil {
		return nil, emptyCloser, err
	}
	ai, err := newAuthInjector(cfg.TVM.ClientID, cfg.TVM.DestinationID, string(cfg.TVM.Secret))
	if err != nil {
		return nil, emptyCloser, xerrors.Errorf("failed to init tvm for logging: %w", err)
	}
	opts := []otlploggrpc.Option{
		otlploggrpc.WithEndpoint(cfg.URI),
		otlploggrpc.WithDialOption(
			grpc.WithPerRPCCredentials(ai),
		),
	}
	return newOtelLogger(ctx, opts, r, cfg.ServiceName)
}

func newOtelUnifiedAgent(ctx context.Context, cfg OtelUnifiedAgent) (log.Logger, func(ctx context.Context), error) {
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

	opts := []otlploggrpc.Option{
		otlploggrpc.WithEndpointURL(cfg.URI),
		otlploggrpc.WithInsecure(),
	}
	return newOtelLogger(ctx, opts, r, cfg.ServiceName)
}

func newOtelLogger(ctx context.Context, opts []otlploggrpc.Option, r *resource.Resource, serviceName string) (log.Logger, func(ctx context.Context), error) {
	logExporter, err := otlploggrpc.New(ctx, opts...)
	if err != nil {
		return noOp, emptyCloser, xerrors.Errorf("failed to create trace exporter: %w", err)
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
	l := log.With(corezap.NewWithCore(core), log.String(KeyApp, serviceName))
	return l, func(ctx context.Context) {
		ctx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		if err := logProvider.Shutdown(ctx); err != nil {
			Log.Error("failed to shutdown otelLogger provider", log.Error(err))
		}
	}, nil
}
