//go:build !disable_mongo_provider

package mongo

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/dbaas"
	"github.com/transferia/transferia/pkg/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/atomic"
	"go.ytsaurus.tech/library/go/core/log"
)

type MongoClientWrapper struct {
	*mongo.Client
	lgr log.Logger
	id  int64

	IsDocDB bool
}

// This erorr might be returned when using AWS DocumentDB, as it does not support the 'type' field
// on db.listCollections(). Checking this error enables a retry without filtering on the 'type'.
const errDocDBTypeUnsupported = "Field 'type' is currently not supported"

const DefaultAuthSource = "admin"

func (w *MongoClientWrapper) Close(ctx context.Context) error {
	miniCallstack := util.GetMiniCallstack(12)
	w.lgr.Info("Closing mongo client wrapper",
		log.Int64("mongo_client_wrapper_id", w.id),
		log.Strings("mini_callstack", miniCallstack))
	if err := w.Disconnect(ctx); err != nil {
		w.lgr.Errorf("cannot disconnect from MongoDB: %v", err)
		return err
	}
	return nil
}

var tmMongoConnectID atomic.Int64

// Connect function should be one and only one valid method of creation mongo client
// this method logs into 'lgr' about connection options
func Connect(ctx context.Context, opts MongoConnectionOptions, lgr log.Logger) (*MongoClientWrapper, error) {
	if lgr == nil {
		lgr = logger.Log
	}

	miniCallstack := util.GetMiniCallstack(12)
	var driverConnectionOptions *options.ClientOptions
	var err error

	if opts.ConnectionID != "" {
		if err := opts.ResolveCredsByConnectionID(ctx); err != nil {
			return nil, xerrors.Errorf("unable to resolve connection credentials by connection ID: %w", err)
		}
	}

	if opts.SRVMode {
		driverConnectionOptions, err = DriverConnectionSrvOptions(&opts)
	} else {
		driverConnectionOptions, err = DriverConnectionOptions(&opts)
	}

	if err != nil {
		return nil, xerrors.Errorf("Cannot prepare connection options for source endpoint: %w", err)
	}
	id := tmMongoConnectID.Inc()
	lgr.Info("Creating mongo client", log.Int64("mongo_client_wrapper_id", id),
		log.String("opts.ClusterID", opts.ClusterID),
		log.String("opts.ConnectionID", opts.ConnectionID),
		log.String("opts.HostsWithPort", fmt.Sprintf("%v", opts.HostsWithPort)),
		log.String("opts.ReplicaSet", opts.ReplicaSet),
		log.Strings("mini_callstack", miniCallstack),
	)
	if opts.User == "" {
		// we cannot expose user, but we can expose absence of requested user
		lgr.Info("Warning: empty User, default will be used",
			log.Int64("mongo_client_wrapper_id", id),
			log.Strings("mini_callstack", miniCallstack),
		)
	}
	if opts.AuthSource == "" {
		// we cannot expose user, but we can expose absence of requested user
		lgr.Info("Warning: empty AuthSource, default will be used",
			log.Int64("mongo_client_wrapper_id", id),
			log.Strings("mini_callstack", miniCallstack))
	}

	// create client
	client, err := newClient(ctx, driverConnectionOptions)
	if err != nil {
		if driverConnectionOptions.TLSConfig != nil {
			lgr.Error("Error initializing mongo client",
				log.Int64("mongo_client_wrapper_id", id),
				log.String("opts.ClusterID", opts.ClusterID),
				log.String("opts.ConnectionID", opts.ConnectionID),
				log.String("opts.HostsWithPort", fmt.Sprintf("%v", opts.HostsWithPort)),
				log.String("opts.ReplicaSet", opts.ReplicaSet),
				log.Strings("mini_callstack", miniCallstack),
				log.Error(err),
			)
			return nil, xerrors.Errorf("unable to create mongo client: %w", err)
		}

		lgr.Warn("Connection failed, try to connect with TLS enforcement even if no TLS cert is specified",
			log.Int64("mongo_client_wrapper_id", id),
			log.String("opts.ClusterID", opts.ClusterID),
			log.String("opts.ConnectionID", opts.ConnectionID),
			log.String("opts.HostsWithPort", fmt.Sprintf("%v", opts.HostsWithPort)),
			log.String("opts.ReplicaSet", opts.ReplicaSet),
			log.Strings("mini_callstack", miniCallstack),
			log.Error(err),
		)
		// tls might be enforced on server side
		driverConnectionOptions.TLSConfig = new(tls.Config)
		clientWithTLS, err := newClient(ctx, driverConnectionOptions)
		if err != nil {
			lgr.Error("Error initializing TLS enforced mongo client",
				log.Int64("mongo_client_wrapper_id", id),
				log.String("opts.ClusterID", opts.ClusterID),
				log.String("opts.ConnectionID", opts.ConnectionID),
				log.String("opts.HostsWithPort", fmt.Sprintf("%v", opts.HostsWithPort)),
				log.String("opts.ReplicaSet", opts.ReplicaSet),
				log.Strings("mini_callstack", miniCallstack),
				log.Error(err),
			)
			return nil, xerrors.Errorf("unable to create mongo client (TLS enforce): %w", err)
		}
		client = clientWithTLS
	}

	return &MongoClientWrapper{
		Client:  client,
		lgr:     lgr,
		id:      id,
		IsDocDB: opts.IsDocDB(),
	}, nil
}

func newClient(ctx context.Context, connOpts *options.ClientOptions) (*mongo.Client, error) {
	client, err := mongo.Connect(ctx, connOpts)

	isWithPassword := bool(connOpts != nil && connOpts.Auth != nil && len(connOpts.Auth.Password) > 0)
	if err != nil && isWithPassword && strings.Contains(err.Error(), connOpts.Auth.Password) {
		err = xerrors.New(strings.ReplaceAll(err.Error(), connOpts.Auth.Password, "<secret>"))

		return nil, xerrors.Errorf("unable to connect mongo client: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, xerrors.Errorf("unable to ping mongo db: %w", err)
	}
	return client, nil
}

func getClusterInfo(endpoint *MongoConnectionOptions) (hosts []string, sharded bool, err error) {
	hosts = make([]string, 0, len(endpoint.HostsWithPort))
	sharded = false

	// On-premise MongoDB
	if endpoint.ClusterID == "" || endpoint.ConnectionID != "" {
		for _, host := range endpoint.HostsWithPort {
			hosts = append(hosts, fmt.Sprintf("%s:%d", host.Host, host.Port))
		}

		return hosts, sharded, nil
	}

	// Managed MongoDB
	provider, err := dbaas.Current()
	if err != nil {
		return nil, false, xerrors.Errorf("unable to get dbaas provider: %w", err)
	}
	clusterHosts, err := dbaas.ResolveClusterHosts(dbaas.ProviderTypeMongodb, endpoint.ClusterID)
	if err != nil {
		return nil, false, xerrors.Errorf("unable to resolve mongodb hosts: %w", err)
	}
	// step of choosing port, shard-ness and type of mongo instances to connect
	port := 0
	shardResolver, err := provider.ShardResolver(dbaas.ProviderTypeMongodb, endpoint.ClusterID)
	if err != nil {
		return nil, false, xerrors.Errorf("unable to init shard resolver: %w", err)
	}
	sharded, err = shardResolver.Sharded()
	if err != nil {
		return nil, false, xerrors.Errorf("unable to resolve sharded: %w", err)
	}
	gatewayType := dbaas.InstanceTypeUnspecified // one of mongod, mongos, mongoinfra

	// useful connection info:
	// https://cloud.yandex.ru/docs/managed-mongodb/concepts/sharding
	if sharded {
		port = 27017 // Default port for sharded MongoDB in MDB
		for _, host := range clusterHosts {
			switch host.Type {
			case dbaas.InstanceTypeMongos, dbaas.InstanceTypeMongoinfra:
				if gatewayType == dbaas.InstanceTypeUnspecified {
					gatewayType = host.Type
				} else if gatewayType != host.Type {
					logger.Log.Warnf("Different sharded entrypoints found %v and %v:", gatewayType, host.Type)
				}
			case dbaas.InstanceTypeMongocfg:
				// this is not the type of host we should connect to...
			}
		}
	} else {
		port = 27018 // Default port for MongoDB in MDB
		gatewayType = dbaas.InstanceTypeMongod
	}
	// make hosts out of the selected type of cluster
	for _, host := range clusterHosts {
		if host.Type == gatewayType {
			hosts = append(hosts, fmt.Sprintf("%s:%d", host.Name, port))
		}
	}

	return hosts, sharded, nil
}

func DriverConnectionSrvOptions(mongoConnectionOptions *MongoConnectionOptions) (*options.ClientOptions, error) {
	if len(mongoConnectionOptions.HostsWithPort) != 1 {
		return nil, xerrors.Errorf("Cannot be empty or more than hosts in srv connection")
	}

	uri := fmt.Sprintf("mongodb+srv://%s:%s@%s", mongoConnectionOptions.User, mongoConnectionOptions.Password, mongoConnectionOptions.HostsWithPort[0].Host)

	clientOptions := options.Client().ApplyURI(uri)
	clientOptions.SetDirect(false)

	authSource := mongoConnectionOptions.AuthSource
	if authSource == "" {
		authSource = DefaultAuthSource
	}

	clientOptions.SetAuth(options.Credential{
		AuthSource: authSource,
		Username:   mongoConnectionOptions.User,
		Password:   string(mongoConnectionOptions.Password),
	})

	if len(mongoConnectionOptions.ReplicaSet) > 0 {
		clientOptions.SetReplicaSet(mongoConnectionOptions.ReplicaSet)
	}

	tlsConfig, err := newTLSConfig(mongoConnectionOptions.CACert)
	if err != nil {
		return nil, xerrors.Errorf("Cannot create TLS configuration: %w", err)
	}
	clientOptions.SetTLSConfig(tlsConfig)
	if mongoConnectionOptions.IsDocDB() {
		clientOptions.SetRetryWrites(false)
	}
	return clientOptions, nil
}

func DriverConnectionOptions(mongoConnectionOptions *MongoConnectionOptions) (*options.ClientOptions, error) {
	opts := options.ClientOptions{}
	hosts, sharded, err := getClusterInfo(mongoConnectionOptions)
	if err != nil {
		return nil, xerrors.Errorf("Cannot get hosts: %w", err)
	}

	opts.SetHosts(hosts)
	authSource := mongoConnectionOptions.AuthSource
	if authSource == "" {
		authSource = DefaultAuthSource
	}
	opts.SetDirect(mongoConnectionOptions.Direct)
	opts.SetAuth(options.Credential{
		AuthSource: authSource,
		// AuthMechanism: "SCRAM-SHA-256",
		Username: mongoConnectionOptions.User,
		Password: string(mongoConnectionOptions.Password),
	})
	if mongoConnectionOptions.ClusterID != "" && !sharded {
		opts.SetReplicaSet("rs01")
	} else if len(mongoConnectionOptions.ReplicaSet) > 0 {
		opts.SetReplicaSet(mongoConnectionOptions.ReplicaSet)
	}

	tlsConfig, err := newTLSConfig(mongoConnectionOptions.CACert)
	if err != nil {
		return nil, xerrors.Errorf("Cannot create TLS configuration: %w", err)
	}
	opts.SetTLSConfig(tlsConfig)
	if mongoConnectionOptions.IsDocDB() {
		opts.SetRetryWrites(false)
	}

	return &opts, nil
}

func newTLSConfig(caCert TrustedCACertificate) (*tls.Config, error) {
	if caCert == nil {
		return nil, nil
	}
	rootCertificates := x509.NewCertPool()
	switch downcasted := caCert.(type) {
	case InlineCACertificatePEM:
		inlineCACert := downcasted
		if ok := rootCertificates.AppendCertsFromPEM(inlineCACert); !ok {
			return nil, xerrors.New("Cannot parse PEM CA certificate")
		}
	case CACertificatePEMFilePaths:
		caFilePaths := downcasted
		for _, cert := range caFilePaths {
			pemFileContent, err := os.ReadFile(string(cert))
			if err != nil {
				return nil, xerrors.Errorf("Cannot read file %s: %w", cert, err)
			}
			if ok := rootCertificates.AppendCertsFromPEM(pemFileContent); !ok {
				return nil, xerrors.Errorf("Cannot parse PEM CA certificate at %s", cert)
			}
		}
	}

	return &tls.Config{RootCAs: rootCertificates}, nil
}

func GetAllExistingCollections(ctx context.Context, client *MongoClientWrapper) ([]MongoCollection, error) {
	var allDatabaseCollections []MongoCollection
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	dbs, err := client.ListDatabaseNames(ctx, emptyFilter)
	if err != nil {
		return nil, xerrors.Errorf("unable to list databases: %w", err)
	}
	dbs = filterSystemDBs(dbs, SystemDBs)

	for _, db := range dbs {
		collectionSpecifications, err := client.Database(db).ListCollectionSpecifications(ctx, CollectionFilter)
		if err != nil {
			// AWS DocumentDB does not support the field 'type' on listCollections()
			if err.Error() != errDocDBTypeUnsupported {
				return nil, xerrors.Errorf("failed to list collection specifications from database %s: %w", db, err)
			}
			collectionSpecifications, err = client.Database(db).ListCollectionSpecifications(context.Background(), bson.D{})
			if err != nil {
				return nil, xerrors.Errorf("failed to list collection specifications from database %s: %w", db, err)
			}
		}

		collections, err := filterSystemCollections(collectionSpecifications)
		if err != nil {
			return nil, xerrors.Errorf("failed to filter out system collections from database %s: %w", db, err)
		}
		for _, collection := range collections {
			allDatabaseCollections = append(allDatabaseCollections, MongoCollection{
				DatabaseName:   db,
				CollectionName: collection,
			})
		}
	}
	return allDatabaseCollections, nil
}
