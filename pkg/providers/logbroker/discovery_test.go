package logbroker

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/yandex/ydb/ydb-topic-fed-sdk/fedtypes"
	"github.com/transferia/transferia/pkg/abstract"
	topiccommon "github.com/transferia/transferia/pkg/providers/ydb/topics/common"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_FederationDiscovery_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_FederationDiscovery"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"go.ytsaurus.tech/library/go/core/log/nop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestResolveFederationInstances(t *testing.T) {
	for _, cluster := range []LogbrokerCluster{Logbroker, LogbrokerPrestable, LogbrokerSerbia} {
		t.Run(string(cluster), func(t *testing.T) {
			cfg := &LfSource{Cluster: cluster, Topics: []string{"account/topic", "account/dir/other"}, Consumer: "account/consumer", Port: 1234}
			original := *cfg
			var connection topiccommon.ConnectionConfig
			instances, err := resolveInstancesWithDiscovery(context.Background(), cfg, &nop.Logger{}, func(_ context.Context, c topiccommon.ConnectionConfig) ([]fedtypes.DatabaseInfo, error) {
				connection = c
				return []fedtypes.DatabaseInfo{
					{Endpoint: "new-dc.example:4567", Path: "/physical/account", Status: fedtypes.DatabaseInfoStatusREADONLY},
					{Endpoint: "old-dc.example:4567", Path: "/other/account", Status: fedtypes.DatabaseInfoStatusUNAVAILABLE},
				}, nil
			})
			require.NoError(t, err)
			require.Equal(t, []LogbrokerInstance{"new-dc.example:4567", "old-dc.example:4567"}, instances)
			require.Equal(t, "/logbroker-federation/account", connection.Database)
			if cluster == LogbrokerSerbia {
				require.Equal(t, "logbroker.yango.tech:2136", connection.Endpoint)
				require.True(t, connection.TLSEnabled)
			} else {
				require.Equal(t, string(cluster)+".yandex.net:1234", connection.Endpoint)
			}
			require.Equal(t, original, *cfg, "discovery must not change endpoint settings")
		})
	}
}

func TestResolveSeveralAccountsUsesOneReaderPerEndpoint(t *testing.T) {
	cfg := &LfSource{Cluster: Logbroker, Database: "/custom", Topics: []string{"b/topic", "a/topic", "a/other"}, Consumer: "c/consumer"}
	calls := make(map[string]int)
	instances, err := resolveInstancesWithDiscovery(context.Background(), cfg, &nop.Logger{}, func(_ context.Context, c topiccommon.ConnectionConfig) ([]fedtypes.DatabaseInfo, error) {
		calls[c.Database]++
		return []fedtypes.DatabaseInfo{
			{Endpoint: "shared.example:2135", Path: "/physical" + c.Database},
			{ID: "same", Endpoint: "other.example:2135", Path: "/other" + c.Database},
			{ID: "same", Endpoint: "other.example:2135", Path: "/other" + c.Database},
		}, nil
	})
	require.NoError(t, err)
	require.Equal(t, map[string]int{"/logbroker-federation/a": 1, "/logbroker-federation/b": 1}, calls)
	require.Equal(t, []LogbrokerInstance{"shared.example:2135", "other.example:2135"}, instances)

	multi := newControlledMultiSource(t)
	multi.cfg = cfg
	for _, instance := range instances {
		child := multi.configForInstance(instance)
		built, err := child.buildTopicSourceConfig()
		require.NoError(t, err)
		require.Equal(t, string(instance), built.Connection.Endpoint)
		require.Equal(t, "/custom", built.Connection.Database)
		require.Equal(t, cfg.Topics, built.Topics)
		require.Equal(t, cfg.Consumer, built.Consumer)
		require.True(t, built.ReaderOpts.PQv1.ReadOnlyLocal)
	}
}

func TestDiscoveredReadersKeepLegacyConnectionSettings(t *testing.T) {
	for _, cluster := range []LogbrokerCluster{Logbroker, LogbrokerSerbia, YcLogbroker} {
		t.Run(string(cluster), func(t *testing.T) {
			cfg := &LfSource{Cluster: cluster, Database: "/custom", Topics: []string{"account/topic"}, Consumer: "account/consumer",
				Port: 1234, TLS: DisabledTLS, MaxMemory: 123456, MaxTimeLag: time.Minute, ParseQueueParallelism: 3, UseTopicAPI: true}
			cfg.WithDefaults()
			multi := newControlledMultiSource(t)
			multi.cfg = cfg
			legacyInstances, ok := ClusterInstances(cluster)
			require.True(t, ok)
			legacy := multi.configForInstance(legacyInstances[0])
			expected, err := legacy.buildTopicSourceConfig()
			require.NoError(t, err)
			child := multi.configForInstance("new.example:4567")
			actual, err := child.buildTopicSourceConfig()
			require.NoError(t, err)
			expected.Connection.Endpoint = "new.example:4567"
			require.Equal(t, expected, actual, "only the reader address may change")
			require.Equal(t, cfg.UseTopicAPI, child.UseTopicAPI)
		})
	}
}

func TestResolveSingleDatabaseInstances(t *testing.T) {
	for _, cfg := range []*LfSource{
		{Instance: "not-in-static-catalog.example", Port: 1234, Database: "/custom", Topics: []string{"topic"}},
		{Cluster: LbkxMow, Topics: []string{"a/topic"}},
		{Cluster: YcLogbroker, Topics: []string{"topic"}},
		{Cluster: LbkxSerbia, Topics: []string{"a/topic"}},
	} {
		instances, err := resolveInstancesWithDiscovery(context.Background(), cfg, &nop.Logger{}, func(context.Context, topiccommon.ConnectionConfig) ([]fedtypes.DatabaseInfo, error) {
			t.Fatal("single database must not invoke federation discovery")
			return nil, nil
		})
		require.NoError(t, err)
		if cfg.Cluster == "" {
			require.Equal(t, []LogbrokerInstance{cfg.Instance}, instances)
		} else {
			expected, _ := ClusterInstances(cfg.Cluster)
			require.Equal(t, expected, instances)
		}
	}
}

func TestResolveInstancesPropagatesErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := resolveInstancesWithDiscovery(ctx, &LfSource{Cluster: Logbroker, Topics: []string{"a/topic"}}, &nop.Logger{}, func(ctx context.Context, _ topiccommon.ConnectionConfig) ([]fedtypes.DatabaseInfo, error) {
		return nil, ctx.Err()
	})
	require.ErrorIs(t, err, context.Canceled)
	_, err = resolveInstances(ctx, &LfSource{Cluster: "unknown"}, &nop.Logger{})
	require.True(t, abstract.IsFatal(err))
}

func TestResolveInstancesRejectsIncompleteTopology(t *testing.T) {
	for _, topics := range [][]string{{"a/topic"}, {"a/topic", "b/topic"}} {
		instances, err := resolveInstancesWithDiscovery(context.Background(), &LfSource{Cluster: Logbroker, Topics: topics}, &nop.Logger{}, func(_ context.Context, c topiccommon.ConnectionConfig) ([]fedtypes.DatabaseInfo, error) {
			if len(topics) > 1 && c.Database == "/logbroker-federation/a" {
				return []fedtypes.DatabaseInfo{{Endpoint: "node.example:2135"}}, nil
			}
			return nil, nil
		})
		require.Error(t, err)
		require.Nil(t, instances, "must not return a partial topology")
	}
}

func TestDiscoveryDeadlineRespectsSamplingBudget(t *testing.T) {
	for _, timeout := range []time.Duration{5 * time.Minute, 20 * time.Second} {
		ctx := context.Background()
		if timeout == 20*time.Second {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		started := time.Now()
		_, err := resolveInstancesWithDiscovery(ctx, &LfSource{Cluster: Logbroker, Topics: []string{"a/topic"}}, &nop.Logger{}, func(ctx context.Context, _ topiccommon.ConnectionConfig) ([]fedtypes.DatabaseInfo, error) {
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.WithinDuration(t, started.Add(timeout), deadline, time.Second)
			return []fedtypes.DatabaseInfo{{Endpoint: "node.example:2135"}}, nil
		})
		require.NoError(t, err)
	}
}

type discoveryServer struct {
	Ydb_FederationDiscovery_V1.UnimplementedFederationDiscoveryServiceServer
	requests chan metadata.MD
}

func (s *discoveryServer) ListFederationDatabases(ctx context.Context, _ *Ydb_FederationDiscovery.ListFederationDatabasesRequest) (*Ydb_FederationDiscovery.ListFederationDatabasesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	select {
	case s.requests <- md:
	default:
	}
	result, err := anypb.New(&Ydb_FederationDiscovery.ListFederationDatabasesResult{FederationDatabases: []*Ydb_FederationDiscovery.DatabaseInfo{
		{Id: "a-new", Endpoint: "new.example:2135", Path: "/physical/a", Status: Ydb_FederationDiscovery.DatabaseInfo_READ_ONLY},
	}})
	if err != nil {
		return nil, err
	}
	return &Ydb_FederationDiscovery.ListFederationDatabasesResponse{Operation: &Ydb_Operations.Operation{Ready: true, Status: Ydb.StatusIds_SUCCESS, Result: result}}, nil
}

func TestDiscoverFederationRPC(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	service := &discoveryServer{requests: make(chan metadata.MD, 2)}
	Ydb_FederationDiscovery_V1.RegisterFederationDiscoveryServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	databases, err := discoverFederation(ctx, topiccommon.ConnectionConfig{
		Endpoint: listener.Addr().String(), Database: "/logbroker-federation/a",
		Credentials: credentials.NewAccessTokenCredentials("synthetic-test-token"),
	})
	require.NoError(t, err)
	require.Len(t, databases, 1)
	require.Equal(t, "/physical/a", databases[0].Path)
	require.Equal(t, fedtypes.DatabaseInfoStatusREADONLY, databases[0].Status)
	select {
	case md := <-service.requests:
		require.Equal(t, []string{"/logbroker-federation/a"}, md.Get("x-ydb-database"))
		require.Equal(t, []string{"synthetic-test-token"}, md.Get("x-ydb-auth-ticket"))
	case <-ctx.Done():
		t.Fatal("discovery request was not received")
	}
}
