package clickhouse

import (
	"context"
	"time"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/abstract2"
	"github.com/transferia/transferia/pkg/data"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers"
	clickhouse_async "github.com/transferia/transferia/pkg/providers/clickhouse/async"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/sink_factory"
	"github.com/transferia/transferia/pkg/targets"
	"github.com/transferia/transferia/pkg/util/gobwrapper"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	gobwrapper.RegisterName("*server.ChSource", new(clickhouse_model.ChSource))
	gobwrapper.RegisterName("*server.ChDestination", new(clickhouse_model.ChDestination))
	model.RegisterDestination(ProviderType, func() model.LoggableDestination {
		return new(clickhouse_model.ChDestination)
	})
	model.RegisterSource(ProviderType, func() model.LoggableSource {
		return new(clickhouse_model.ChSource)
	})

	abstract.RegisterProviderName(ProviderType, "ClickHouse")
	providers.Register(ProviderType, New)
}

const ProviderType = abstract.ProviderType("ch")

// To verify providers contract implementation
var (
	_ providers.Snapshot          = (*Provider)(nil)
	_ providers.Abstract2Provider = (*Provider)(nil)
	_ providers.AsyncSinker       = (*Provider)(nil)
	_ providers.Sinker            = (*Provider)(nil)
	_ providers.Abstract2Sinker   = (*Provider)(nil)
	_ providers.Tester            = (*Provider)(nil)
	_ providers.Activator         = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry core_metrics.Registry
	cp       coordinator.Coordinator
	transfer *model.Transfer
}

func (p *Provider) Target(options ...abstract.SinkOption) (abstract2.EventTarget, error) {
	if _, ok := p.transfer.Src.(*clickhouse_model.ChSource); !ok {
		return nil, targets.UnknownTargetError
	}
	return NewHTTPTarget(p.transfer, p.registry, p.logger)
}

func (p *Provider) Sink(config middlewares.Config) (abstract.Sinker, error) {
	s, err := NewSink(p.transfer, p.logger, p.registry, config)
	if err != nil {
		return nil, xerrors.Errorf("failed to create ClickHouse sinker: %w", err)
	}
	return s, nil
}

func (p *Provider) AsyncSink(middleware abstract.Middleware) (abstract.AsyncSink, error) {
	if p.transfer.IsAsyncCHExp() {
		p.logger.Warn("Using experimental asynchronous ClickHouse sink")
		sink, err := clickhouse_async.NewSink(p.transfer, p.transfer.Dst.(*clickhouse_model.ChDestination), p.logger, p.registry, middleware)
		if err != nil {
			return nil, xerrors.Errorf("error getting experimental asynchronous ClickHouse sink: %w", err)
		}
		return sink, nil
	}
	return nil, sink_factory.NoAsyncSinkErr
}

func (p *Provider) Storage() (abstract.Storage, error) {
	src, ok := p.transfer.Src.(*clickhouse_model.ChSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
	}
	chOpts := []StorageOpt{WithMetrics(p.registry), WithTableFilter(src)}
	if _, ok := p.transfer.Dst.(*clickhouse_model.ChDestination); ok {
		chOpts = append(chOpts, WithHomo())
	}
	storageParams, err := src.ToStorageParams()
	if err != nil {
		return nil, xerrors.Errorf("unable to resole storage params: %w", err)
	}

	storage, err := NewStorage(storageParams, p.transfer, chOpts...)
	if err != nil {
		return nil, xerrors.Errorf("failed to create a ClickHouse storage: %w", err)
	}
	return storage, nil
}

func (p *Provider) DataProvider() (abstract2.DataProvider, error) {
	specificConfig, ok := p.transfer.Src.(*clickhouse_model.ChSource)
	if !ok {
		return nil, xerrors.Errorf("Unexpected source type: %T", p.transfer.Src)
	}
	if p.transfer.DstType() != ProviderType {
		return nil, data.TryLegacySourceError // just for homo
	}
	return NewClickhouseProvider(p.logger, p.registry, specificConfig, p.transfer)
}

func (p *Provider) Activate(_ context.Context, _ *model.TransferOperation, tables abstract.TableMap, callbacks providers.ActivateCallbacks) error {
	if !p.transfer.SnapshotOnly() {
		return xerrors.New("Only allowed mode for CH source is snapshot")
	}
	if err := callbacks.Cleanup(tables); err != nil {
		return xerrors.Errorf("Sinker cleanup failed: %w", err)
	}
	if err := callbacks.CheckIncludes(tables); err != nil {
		return xerrors.Errorf("Failed in accordance with configuration: %w", err)
	}
	if err := p.loadClickHouseSchema(); err != nil {
		return xerrors.Errorf("Cannot load schema from source database: %w", err)
	}
	if err := callbacks.Upload(tables); err != nil {
		return xerrors.Errorf("Snapshot loading failed: %w", err)
	}
	return nil
}

func (p *Provider) Type() abstract.ProviderType {
	return ProviderType
}

const (
	CredentialsCheckType = abstract.CheckType("credentials")
	ConnectivityNative   = abstract.CheckType("connection-native")
	ConnectivityHTTP     = abstract.CheckType("connection-http")
)

func (p *Provider) TestChecks() []abstract.CheckType {
	return []abstract.CheckType{ConnectivityHTTP, ConnectivityNative, CredentialsCheckType}
}

func (p *Provider) Test(ctx context.Context) *abstract.TestResult {
	src, ok := p.transfer.Src.(*clickhouse_model.ChSource)
	if !ok {
		return nil
	}

	tr := abstract.NewTestResult(p.TestChecks()...)
	// Native connect
	storageParams, err := src.ToStorageParams()
	if err != nil {
		return tr.NotOk(ConnectivityNative, xerrors.Errorf("unable to resolve storage params: %w", err))
	}
	db, err := MakeConnection(storageParams)
	if err != nil {
		return tr.NotOk(ConnectivityNative, xerrors.Errorf("unable to init a CH storage: %w", err))
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		return tr.NotOk(ConnectivityNative, xerrors.Errorf("unable to reach ClickHouse: %w", err))
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return tr.NotOk(ConnectivityNative, xerrors.Errorf("unable to establish a connection to ClickHouse: %w", err))
	}
	defer conn.Close()

	err = conn.PingContext(ctx)
	if err != nil {
		return tr.NotOk(ConnectivityNative, xerrors.Errorf("unable to ping a connection to ClickHouse: %w", err))
	}

	shortContext, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	rows, err := conn.QueryContext(shortContext, "SELECT 1;")
	if err != nil || rows == nil {
		return tr.NotOk(CredentialsCheckType, xerrors.Errorf("unable to query ClickHouse: %w", err))
	}
	defer rows.Close()
	tr.Ok(CredentialsCheckType)

	err = conn.Close()
	if err != nil {
		return tr.NotOk(ConnectivityNative, xerrors.Errorf("unable to close a connection to ClickHouse: %w", err))
	}

	tr.Ok(ConnectivityNative)

	// HTTP connect
	storageParams, err = src.ToStorageParams()
	if err != nil {
		return tr.NotOk(ConnectivityNative, xerrors.Errorf("unable to resolve storage params: %w", err))
	}
	cl, err := httpclient.NewHTTPClientImpl(storageParams.ToConnParams())
	if err != nil {
		return tr.NotOk(ConnectivityHTTP, xerrors.Errorf("unable to create ClickHouse client: %w", err))
	}
	var res uint64
	shards := storageParams.ConnectionParams.Shards

	for _, shardHosts := range shards {
		for _, host := range shardHosts {
			err = cl.Query(context.Background(), p.logger, host, "SELECT 1;", &res, nil)
			if err != nil {
				return tr.NotOk(ConnectivityHTTP, xerrors.Errorf("unable to query ClickHouse host: %s err: %w", host.Name, err))
			}
			p.logger.Infof("host is reachable! host: %s", host.Name)
		}
	}

	tr.Ok(ConnectivityHTTP)
	return tr
}

func New(lgr log.Logger, registry core_metrics.Registry, cp coordinator.Coordinator, transfer *model.Transfer, _ *model.TransferOperation) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}
