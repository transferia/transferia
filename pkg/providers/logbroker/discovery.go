package logbroker

import (
	"context"
	"path"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	ydbfed "github.com/transferia/transferia/library/go/yandex/ydb/ydb-topic-fed-sdk"
	"github.com/transferia/transferia/library/go/yandex/ydb/ydb-topic-fed-sdk/fedtypes"
	"github.com/transferia/transferia/pkg/abstract"
	topiccommon "github.com/transferia/transferia/pkg/providers/ydb/topics/common"
	"github.com/transferia/transferia/pkg/xtls"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"go.ytsaurus.tech/library/go/core/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const federationDatabasePrefix = "/logbroker-federation"
const discoveryTimeout = 5 * time.Minute

type federationDiscovery func(context.Context, topiccommon.ConnectionConfig) ([]fedtypes.DatabaseInfo, error)

// resolveInstances discovers addresses only. Readers keep their original
// database, consumer and complete topic list, with one reader per endpoint.
func resolveInstances(ctx context.Context, cfg *LfSource, logger log.Logger) ([]LogbrokerInstance, error) {
	return resolveInstancesWithDiscovery(ctx, cfg, logger, discoverFederation)
}

func resolveInstancesWithDiscovery(ctx context.Context, cfg *LfSource, logger log.Logger, discover federationDiscovery) ([]LogbrokerInstance, error) {
	if cfg.Cluster == "" {
		return []LogbrokerInstance{cfg.Instance}, nil
	}
	installation, ok := installationByCluster(cfg.Cluster)
	if !ok {
		return nil, abstract.NewFatalError(xerrors.Errorf("unknown Logbroker installation %q", cfg.Cluster))
	}
	if !installation.federated {
		return append([]LogbrokerInstance(nil), installation.instances...), nil
	}
	// Federation membership may differ between accounts. Query each account and
	// union its endpoints, without switching readers to account-scoped databases.
	accounts := make(map[string]bool)
	for _, topic := range cfg.Topics {
		account, _, err := extractAccountAndRelativeTopic(topic)
		if err != nil {
			return nil, abstract.NewFatalError(err)
		}
		accounts[account] = true
	}
	bootstrap := *cfg
	bootstrap.Instance = LogbrokerInstance(installation.endpoint)
	bootstrapConfig, err := bootstrap.buildTopicSourceConfig()
	if err != nil {
		return nil, abstract.NewFatalError(err)
	}
	ctx, cancel := context.WithTimeout(ctx, discoveryTimeout)
	defer cancel()
	var result []LogbrokerInstance
	seen := make(map[string]bool)
	for account := range accounts {
		connection := bootstrapConfig.Connection
		connection.Database = path.Join(federationDatabasePrefix, account)
		databases, err := discover(ctx, connection)
		if err != nil {
			return nil, xerrors.Errorf("discover Logbroker installation %q, account %q: %w", cfg.Cluster, account, err)
		}
		// Every account must contribute readers; accepting an empty list here
		// would silently omit its data even if other accounts were discovered.
		if len(databases) == 0 {
			return nil, xerrors.Errorf("Logbroker discovery returned no databases for account %q", account)
		}
		for _, database := range databases {
			// Include READ_ONLY and temporarily unavailable databases: they may
			// still contain unconsumed data. Write availability is not read completion.
			if seen[database.Endpoint] {
				continue
			}
			seen[database.Endpoint] = true
			logger.Info("discovered Logbroker instance", log.String("endpoint", database.Endpoint))
			result = append(result, LogbrokerInstance(database.Endpoint))
		}
	}
	return result, nil
}

func discoverFederation(ctx context.Context, cfg topiccommon.ConnectionConfig) ([]fedtypes.DatabaseInfo, error) {
	opts := []ydbfed.Option{}
	if cfg.Credentials != nil {
		opts = append(opts, ydbfed.WithCredentials(cfg.Credentials))
	}
	if cfg.TLSEnabled {
		tlsConfig, err := xtls.FromPath(cfg.RootCAFiles)
		if err != nil {
			return nil, xerrors.Errorf("configure discovery TLS: %w", err)
		}
		opts = append(opts, ydbfed.WithGrpcOptions(grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))),
			ydbfed.WithYDBOptions(ydb.WithTLSConfig(tlsConfig)))
	}
	driver, err := ydbfed.Open(ctx, sugar.DSN(cfg.Endpoint, cfg.Database, sugar.WithSecure(cfg.TLSEnabled)), opts...)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = driver.Close(ctx)
	}()
	return driver.GetStateDBInfos(), nil
}
