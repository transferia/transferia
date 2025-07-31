package yc

import (
	"context"

	iampb "github.com/transferia/transferia/cloud/bitbucket/private-api/yandex/cloud/priv/iam/v1"
	"github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/mdb/clickhouse/v1"
	"github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/mdb/elasticsearch/v1"
	"github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/mdb/greenplum/v1"
	"github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/mdb/kafka/v1"
	"github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/mdb/mongodb/v1"
	"github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/mdb/mysql/v1"
	"github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/mdb/opensearch/v1"
	"github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/mdb/postgresql/v1"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/auth/resources"
	"google.golang.org/grpc"
)

type PgClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *PgClient) ListHosts(ctx context.Context, in *postgresql.ListClusterHostsRequest, opts ...grpc.CallOption) (*postgresql.ListClusterHostsResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return WithAgentFromOpts(ctx, postgresql.NewClusterServiceClient(conn).ListHosts, in, &iampb.Resource{
		Id:   in.GetClusterId(),
		Type: resources.ResourceMdbPostgresqlCluster,
	}, opts...)
}

type GreenplumClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *GreenplumClient) ListMasterHosts(ctx context.Context, in *greenplum.ListClusterHostsRequest, opts ...grpc.CallOption) (*greenplum.ListClusterHostsResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return WithAgentFromOpts(ctx, greenplum.NewClusterServiceClient(conn).ListMasterHosts, in, &iampb.Resource{
		Id:   in.GetClusterId(),
		Type: resources.ResourceMdbGreenplumCluster,
	}, opts...)
}

func (c *GreenplumClient) ListSegmentHosts(ctx context.Context, in *greenplum.ListClusterHostsRequest, opts ...grpc.CallOption) (*greenplum.ListClusterHostsResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return WithAgentFromOpts(ctx, greenplum.NewClusterServiceClient(conn).ListSegmentHosts, in, &iampb.Resource{
		Id:   in.GetClusterId(),
		Type: resources.ResourceMdbGreenplumCluster,
	}, opts...)
}

type MysqlClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *MysqlClient) ListHosts(ctx context.Context, in *mysql.ListClusterHostsRequest, opts ...grpc.CallOption) (*mysql.ListClusterHostsResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return WithAgentFromOpts(ctx, mysql.NewClusterServiceClient(conn).ListHosts, in, &iampb.Resource{
		Id:   in.GetClusterId(),
		Type: resources.ResourceMdbMysqlCluster,
	}, opts...)
}

type ClickHouseClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *ClickHouseClient) ListHosts(ctx context.Context, in *clickhouse.ListClusterHostsRequest, opts ...grpc.CallOption) (*clickhouse.ListClusterHostsResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return WithAgentFromOpts(ctx, clickhouse.NewClusterServiceClient(conn).ListHosts, in, &iampb.Resource{
		Id:   in.GetClusterId(),
		Type: resources.ResourceMdbClickHouseCluster,
	}, opts...)
}

type MongoDBClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *MongoDBClient) ListHosts(ctx context.Context, in *mongodb.ListClusterHostsRequest, opts ...grpc.CallOption) (*mongodb.ListClusterHostsResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return WithAgentFromOpts(ctx, mongodb.NewClusterServiceClient(conn).ListHosts, in, &iampb.Resource{
		Id:   in.GetClusterId(),
		Type: resources.ResourceMdbMongodbCluster,
	}, opts...)
}

type KafkaClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *KafkaClient) ListHosts(ctx context.Context, in *kafka.ListClusterHostsRequest, opts ...grpc.CallOption) (*kafka.ListClusterHostsResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return WithAgentFromOpts(ctx, kafka.NewClusterServiceClient(conn).ListHosts, in, &iampb.Resource{
		Id:   in.GetClusterId(),
		Type: resources.ResourceMdbKafkaCluster,
	}, opts...)
}

type ElasticSearchClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *ElasticSearchClient) ListHosts(ctx context.Context, in *elasticsearch.ListClusterHostsRequest, opts ...grpc.CallOption) (*elasticsearch.ListClusterHostsResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return WithAgentFromOpts(ctx, elasticsearch.NewClusterServiceClient(conn).ListHosts, in, &iampb.Resource{
		Id:   in.GetClusterId(),
		Type: resources.ResourceMdbElasticsearchCluster,
	}, opts...)
}

type OpenSearchClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *OpenSearchClient) hasRole(host *opensearch.Host, expectedRole opensearch.OpenSearch_GroupRole) bool {
	for _, currRole := range host.Roles {
		if currRole == expectedRole {
			return true
		}
	}
	return false
}

func (c *OpenSearchClient) ListDataHosts(ctx context.Context, in *opensearch.ListClusterHostsRequest, opts ...grpc.CallOption) (*opensearch.ListClusterHostsResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection, err: %w", err)
	}
	allHosts, err := WithAgentFromOpts(ctx, opensearch.NewClusterServiceClient(conn).ListHosts, in, &iampb.Resource{
		Id:   in.GetClusterId(),
		Type: resources.ResourceMdbOpensearchCluster,
	}, opts...)
	if err != nil {
		return nil, xerrors.Errorf("unable to list OpenSearch hosts, err: %w", err)
	}
	result := new(opensearch.ListClusterHostsResponse)
	for _, currHost := range allHosts.Hosts {
		if c.hasRole(currHost, opensearch.OpenSearch_DATA) {
			result.Hosts = append(result.Hosts, currHost)
		}
	}
	return result, nil
}

type MDB struct {
	sdk *SDK
}

func (m *MDB) PostgreSQL() *PgClient {
	return &PgClient{
		getConn: m.sdk.getConn(ManagedPostgresqlServiceID),
	}
}

func (m *MDB) Greenplum() *GreenplumClient {
	return &GreenplumClient{
		getConn: m.sdk.getConn(ManagedGreenplumServiceID),
	}
}

func (m *MDB) MySQL() *MysqlClient {
	return &MysqlClient{
		getConn: m.sdk.getConn(ManagedMysqlServiceID),
	}
}

func (m *MDB) ClickHouse() *ClickHouseClient {
	return &ClickHouseClient{
		getConn: m.sdk.getConn(ManagedClickhouseServiceID),
	}
}

func (m *MDB) MongoDB() *MongoDBClient {
	return &MongoDBClient{
		getConn: m.sdk.getConn(ManagedMongodbServiceID),
	}
}

func (m *MDB) Kafka() *KafkaClient {
	return &KafkaClient{
		getConn: m.sdk.getConn(ManagedKafkaServiceID),
	}
}

func (m *MDB) ElasticSearch() *ElasticSearchClient {
	return &ElasticSearchClient{
		getConn: m.sdk.getConn(ManagedElasticsearchServiceID),
	}
}

func (m *MDB) OpenSearch() *OpenSearchClient {
	return &OpenSearchClient{
		getConn: m.sdk.getConn(ManagedOpensearchServiceID),
	}
}
