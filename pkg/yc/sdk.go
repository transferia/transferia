package yc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/endpoint"
	iampb "github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/iam/v1"
	"github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/kms/v1"
	"github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/lockbox/v1"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	DefaultPageSize int64 = 1000
)

type Endpoint string

// To list all the services with their endpoints, run:
// grpcurl -H "Authorization: Bearer $(yc iam create-token)" api.cloud-preprod.yandex.net:443 yandex.cloud.endpoint.ApiEndpointService/List
const (
	AiSpeechkitServiceID             Endpoint = "ai-speechkit"
	AiSttServiceID                   Endpoint = "ai-stt"
	AiTranslateServiceID             Endpoint = "ai-translate"
	AiVisionServiceID                Endpoint = "ai-vision"
	AlbServiceID                     Endpoint = "alb"
	ApplicationLoadBalancerServiceID Endpoint = "application-load-balancer"
	ApploadbalancerServiceID         Endpoint = "apploadbalancer"
	CertificateManagerServiceID      Endpoint = "certificate-manager"
	CertificateManagerDataServiceID  Endpoint = "certificate-manager-data"
	ComputeServiceID                 Endpoint = "compute"
	ContainerRegistryServiceID       Endpoint = "container-registry"
	DataprocServiceID                Endpoint = "dataproc"
	DataprocManagerServiceID         Endpoint = "dataproc-manager"
	DNSServiceID                     Endpoint = "dns"
	EndpointServiceID                Endpoint = "endpoint"
	IamServiceID                     Endpoint = "iam"
	IotDataServiceID                 Endpoint = "iot-data"
	IotDevicesServiceID              Endpoint = "iot-devices"
	K8sServiceID                     Endpoint = "k8s"
	KmsServiceID                     Endpoint = "kms"
	KmsCryptoServiceID               Endpoint = "kms-crypto"
	LoadBalancerServiceID            Endpoint = "load-balancer"
	LocatorServiceID                 Endpoint = "locator"
	LockboxServiceID                 Endpoint = "lockbox"
	LockboxPayloadServiceID          Endpoint = "lockbox-payload"
	LogsServiceID                    Endpoint = "logs"
	ManagedClickhouseServiceID       Endpoint = "managed-clickhouse"
	ManagedElasticsearchServiceID    Endpoint = "managed-elasticsearch"
	ManagedKafkaServiceID            Endpoint = "managed-kafka"
	ManagedKubernetesServiceID       Endpoint = "managed-kubernetes"
	ManagedMongodbServiceID          Endpoint = "managed-mongodb"
	ManagedMysqlServiceID            Endpoint = "managed-mysql"
	ManagedOpensearchServiceID       Endpoint = "managed-opensearch"
	ManagedPostgresqlServiceID       Endpoint = "managed-postgresql"
	ManagedGreenplumServiceID        Endpoint = "managed-greenplum"
	ManagedRedisServiceID            Endpoint = "managed-redis"
	ManagedSqlserverServiceID        Endpoint = "managed-sqlserver"
	MarketplaceServiceID             Endpoint = "marketplace"
	MdbClickhouseServiceID           Endpoint = "mdb-clickhouse"
	MdbMongodbServiceID              Endpoint = "mdb-mongodb"
	MdbMysqlServiceID                Endpoint = "mdb-mysql"
	MdbPostgresqlServiceID           Endpoint = "mdb-postgresql"
	MdbRedisServiceID                Endpoint = "mdb-redis"
	OperationServiceID               Endpoint = "operation"
	ResourceManagerServiceID         Endpoint = "resource-manager"
	ResourcemanagerServiceID         Endpoint = "resourcemanager"
	SerialsshServiceID               Endpoint = "serialssh"
	ServerlessApigatewayServiceID    Endpoint = "serverless-apigateway"
	ServerlessFunctionsServiceID     Endpoint = "serverless-functions"
	ServerlessTriggersServiceID      Endpoint = "serverless-triggers"
	StorageServiceID                 Endpoint = "storage-api"
	VpcServiceID                     Endpoint = "vpc"
	YdbServiceID                     Endpoint = "ydb"
)

// Config is a config that is used to create SDK instance.
type Config struct {
	// Credentials are used to authenticate the client. See Credentials for more info.
	Credentials Credentials
	// DialContextTimeout specifies timeout of dial on API endpoint that
	// is used when building an SDK instance.
	DialContextTimeout time.Duration
	// TLSConfig is optional tls.Config that one can use in order to tune TLS options.
	TLSConfig *tls.Config

	// Endpoint is an API endpoint of Yandex.Cloud against which the SDK is used.
	// Most users won't need to explicitly set it.
	Endpoint  string
	Plaintext bool
}

func DefaultConfig() Config {
	return Config{
		DialContextTimeout: time.Second * 30,
		TLSConfig:          &tls.Config{},
		Plaintext:          false,
		Credentials:        nil,
		Endpoint:           "",
	}
}

func MakeConfig(credentials Credentials, Endpoint string) Config {
	return Config{
		Credentials:        credentials,
		DialContextTimeout: 0,
		TLSConfig:          nil,
		Endpoint:           Endpoint,
		Plaintext:          false,
	}
}

// SDK is a Yandex.Cloud SDK
type SDK struct {
	conf      Config
	cc        ConnContext
	endpoints struct {
		initDone bool
		mu       sync.Mutex
		ep       map[Endpoint]*endpoint.ApiEndpoint
	}

	initErr  error
	initCall Call
	muErr    sync.Mutex
}

// Build creates an SDK instance
func Build(conf Config, customOpts ...grpc.DialOption) (*SDK, error) {
	if conf.Credentials == nil {
		return nil, errors.New("credentials required")
	}

	const DefaultTimeout = 20 * time.Second
	if conf.DialContextTimeout == 0 {
		conf.DialContextTimeout = DefaultTimeout
	}

	switch creds := conf.Credentials.(type) {
	case ExchangeableCredentials, NonExchangeableCredentials:
	default:
		return nil, fmt.Errorf("unsupported credentials type %T", creds)
	}
	var dialOpts []grpc.DialOption

	dialOpts = append(dialOpts, grpc.WithContextDialer(NewProxyDialer(NewDialer())))

	rpcCreds := newRPCCredentials(conf.Plaintext)
	dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(rpcCreds))
	if conf.Plaintext {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	} else {
		tlsConfig := conf.TLSConfig
		if tlsConfig == nil {
			tlsConfig = &tls.Config{}
		}
		creds := credentials.NewTLS(tlsConfig)
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
	}
	// Append custom options after default, to allow to customize dialer and etc.
	dialOpts = append(dialOpts, customOpts...)

	cc := NewLazyConnContext(DialOptions(dialOpts...))
	sdk := &SDK{
		conf: conf,
		cc:   cc,
		endpoints: struct {
			initDone bool
			mu       sync.Mutex
			ep       map[Endpoint]*endpoint.ApiEndpoint
		}{
			initDone: false,
			mu:       sync.Mutex{},
			ep:       nil,
		},
		initErr: nil,
		initCall: Call{
			mu:        sync.Mutex{},
			callState: nil,
		},
		muErr: sync.Mutex{},
	}
	rpcCreds.Init(sdk.CreateIAMToken)
	return sdk, nil
}

// Shutdown shutdowns SDK and closes all open connections.
func (sdk *SDK) Shutdown(ctx context.Context) error {
	return sdk.cc.Shutdown(ctx)
}

// revive:enable:var-naming
func (sdk *SDK) Resolve(ctx context.Context, r ...Resolver) error {
	args := make([]func() error, len(r))
	for k, v := range r {
		resolver := v
		args[k] = func() error {
			return resolver.Run(ctx, sdk)
		}
	}
	return CombineGoroutines(args...)
}

func (sdk *SDK) getConn(serviceID Endpoint) func(ctx context.Context) (*grpc.ClientConn, error) {
	return func(ctx context.Context) (*grpc.ClientConn, error) {
		if !sdk.initDone() {
			sdk.initCall.Do(func() interface{} {
				sdk.muErr.Lock()
				sdk.initErr = sdk.initConns(ctx)
				sdk.muErr.Unlock()
				return nil
			})
			if err := sdk.InitErr(); err != nil {
				return nil, xerrors.Errorf("cannot initialize SDK: %w", err)
			}
		}
		endpoint, endpointExist := sdk.Endpoint(serviceID)
		if !endpointExist {
			return nil, fmt.Errorf("server doesn't know service \"%v\". Known services: %v",
				serviceID,
				sdk.KnownServices())
		}
		return sdk.cc.GetConn(ctx, endpoint.Address)
	}
}

func (sdk *SDK) initDone() (b bool) {
	sdk.endpoints.mu.Lock()
	b = sdk.endpoints.initDone
	sdk.endpoints.mu.Unlock()
	return b
}

func (sdk *SDK) KnownServices() []string {
	sdk.endpoints.mu.Lock()
	result := make([]string, 0, len(sdk.endpoints.ep))
	for k := range sdk.endpoints.ep {
		result = append(result, string(k))
	}
	sdk.endpoints.mu.Unlock()
	sort.Strings(result)
	return result
}

func (sdk *SDK) Endpoint(endpointName Endpoint) (ep *endpoint.ApiEndpoint, exist bool) {
	sdk.endpoints.mu.Lock()
	ep, exist = sdk.endpoints.ep[endpointName]
	sdk.endpoints.mu.Unlock()
	return ep, exist
}

func (sdk *SDK) InitErr() error {
	sdk.muErr.Lock()
	defer sdk.muErr.Unlock()
	return sdk.initErr
}

func (sdk *SDK) initConns(ctx context.Context) error {
	discoveryConn, err := sdk.cc.GetConn(ctx, sdk.conf.Endpoint)
	if err != nil {
		return xerrors.Errorf("cannot get connection: %w", err)
	}
	ec := endpoint.NewApiEndpointServiceClient(discoveryConn)
	const defaultEndpointPageSize = 100
	listResponse, err := ec.List(ctx, &endpoint.ListApiEndpointsRequest{
		PageSize: defaultEndpointPageSize,
	})
	if err != nil {
		return xerrors.Errorf("cannot list API endpoints: %w", err)
	}
	sdk.endpoints.mu.Lock()
	sdk.endpoints.ep = make(map[Endpoint]*endpoint.ApiEndpoint, len(listResponse.Endpoints))
	for _, e := range listResponse.Endpoints {
		sdk.endpoints.ep[Endpoint(e.Id)] = e
	}
	sdk.endpoints.initDone = true
	sdk.endpoints.mu.Unlock()
	return nil
}

// IAM returns IAM object that is used to operate on Yandex Cloud Identity and Access Manager
func (sdk *SDK) IAM() *IAM {
	return NewIAM(sdk.getConn(IamServiceID))
}

// InstanceGroup returns InstanceGroup object that is used to operate on Yandex Compute InstanceGroup
func (sdk *SDK) InstanceGroup() *InstanceGroup {
	return NewInstanceGroup(sdk.getConn(ComputeServiceID))
}

func (sdk *SDK) ResourceManager() *ResourceManager {
	return NewResourceManager(sdk.getConn(ResourceManagerServiceID))
}

func (sdk *SDK) Compute() *Compute {
	return NewCompute(sdk.getConn(ComputeServiceID))
}

func (sdk *SDK) VPC() *VPC {
	return NewVPC(sdk.getConn(VpcServiceID))
}

func (sdk *SDK) MDB() *MDB {
	return &MDB{sdk: sdk}
}

func (sdk *SDK) YDB() *YDB {
	return &YDB{getConn: sdk.getConn(YdbServiceID)}
}

func (sdk *SDK) Serverless() *Serverless {
	return &Serverless{getConn: sdk.getConn(ServerlessFunctionsServiceID)}
}

func (sdk *SDK) Storage() *Storage {
	return &Storage{getConn: sdk.getConn(StorageServiceID)}
}

func (sdk *SDK) Operation() *OperationServiceClient {
	return &OperationServiceClient{getConn: sdk.getConn(OperationServiceID)}
}

func (sdk *SDK) CreateIAMToken(ctx context.Context) (*iampb.CreateIamTokenResponse, error) {
	creds := sdk.conf.Credentials
	switch creds := creds.(type) {
	case ExchangeableCredentials:
		req, err := creds.IAMTokenRequest()
		if err != nil {
			return nil, xerrors.Errorf("cannot create IAM token request: %w", err)
		}
		response, err := sdk.IAM().IamToken().Create(ctx, req)
		if err != nil {
			return nil, xerrors.Errorf("cannot create token from exchangeable credentials: %w", err)
		}
		return response, nil
	case NonExchangeableCredentials:
		response, err := creds.IAMToken(ctx)
		if err != nil {
			return nil, xerrors.Errorf("cannot create token from non exchangeable credentials: %w", err)
		}
		return response, nil
	default:
		return nil, fmt.Errorf("credentials type %T is not supported yet", creds)
	}
}

func (sdk *SDK) KmsSymmetricCryptoServiceClient(ctx context.Context) (kms.SymmetricCryptoServiceClient, error) {
	connect := sdk.getConn(KmsCryptoServiceID)
	clientConn, err := connect(ctx)
	if err != nil {
		return nil, xerrors.Errorf("can't connect to kms symmetric crypto service: %w", err)
	}
	return kms.NewSymmetricCryptoServiceClient(clientConn), nil
}

func (sdk *SDK) LockboxPayloadServiceClient(ctx context.Context) (lockbox.PayloadServiceClient, error) {
	connect := sdk.getConn(LockboxPayloadServiceID)
	clientConn, err := connect(ctx)
	if err != nil {
		return nil, xerrors.Errorf("can't connect to lockbox payload service: %w", err)
	}
	return lockbox.NewPayloadServiceClient(clientConn), nil
}
