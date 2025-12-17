package kafka

import (
	"context"
	"net"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/connection"
	"github.com/transferia/transferia/pkg/connection/kafka"
	"github.com/transferia/transferia/pkg/dbaas"
	"github.com/transferia/transferia/pkg/providers/kafka/client"
	"github.com/transferia/transferia/pkg/util"
)

func ResolveBrokers(s *KafkaConnectionOptions) ([]string, error) {
	var brokers []string
	if s.ClusterID != "" {
		hosts, err := dbaas.ResolveClusterHosts(dbaas.ProviderTypeKafka, s.ClusterID)
		if err != nil {
			return nil, xerrors.Errorf("cannot get hosts for cluster %s: %w", s.ClusterID, err)
		}
		brokers = yslices.Map(hosts, func(t dbaas.ClusterHost) string {
			return t.Name
		})
	} else {
		brokers = s.Brokers
	}
	if len(brokers) == 0 {
		return nil, abstract.NewFatalError(xerrors.New("unable to connect, no brokers found"))
	}
	return brokers, nil
}

func ResolveOnPremBrokers(connectionOpt *KafkaConnectionOptions, kafkaAuth *KafkaAuth, dial func(ctx context.Context, network string, address string) (net.Conn, error)) ([]string, error) {
	tls, err := connectionOpt.TLSConfig()
	if err != nil {
		return nil, xerrors.Errorf("Can't create TLSConfig: %w", err)
	}
	auth, err := kafkaAuth.GetAuthMechanism()
	if err != nil {
		return nil, xerrors.Errorf("Can't get auth mechanism: %w", err)
	}
	kafkaClient, err := client.NewClient(connectionOpt.Brokers, auth, tls, dial)
	if err != nil {
		return nil, xerrors.Errorf("unable to create kafka client, err: %w", err)
	}
	connection, err := kafkaClient.CreateBrokerConn()
	if err != nil {
		return nil, xerrors.Errorf("Can't create kafka connection: %w", err)
	}
	defer connection.Close()
	gotBrokers, err := connection.Brokers()
	if err != nil {
		return nil, xerrors.Errorf("Can't get brokers: %w", err)
	}
	var brokerList = make([]string, 0, len(gotBrokers))
	for _, broker := range gotBrokers {
		brokerList = append(brokerList, broker.Host)
	}
	return brokerList, nil
}

func ResolvePassword(s *KafkaConnectionOptions, kafkaAuth *KafkaAuth) (string, error) {
	password := kafkaAuth.Password

	if s.ClusterID == "" {
		return password, nil
	}

	if kafkaAuth.User != DefaultAuth {
		return password, nil
	}

	provider, err := dbaas.Current()
	if err != nil {
		return "", xerrors.Errorf("unable to resolve provider: %w", err)
	}

	resolver, err := provider.PasswordResolver(dbaas.ProviderTypeKafka, s.ClusterID)
	if err != nil {
		if xerrors.Is(err, dbaas.NotSupported) {
			return password, nil
		}
		return "", xerrors.Errorf("unable to init password resolver: %w", err)
	}

	password, err = resolver.ResolvePassword()
	if err != nil {
		return "", xerrors.Errorf("cannot resolve kafka password: %w", err)
	}

	return password, nil
}

func ResolveConnectionOptions(connection *KafkaConnectionOptions, kafkaConnection *kafka.Connection) *KafkaConnectionOptions {
	if kafkaConnection == nil {
		return connection
	}

	tls := model.DisabledTLS
	if kafkaConnection.HasTLS {
		tls = model.EnabledTLS
	}

	kafkaOptions := &KafkaConnectionOptions{
		ClusterID:      kafkaConnection.ClusterID,
		TLS:            tls,
		TLSFile:        kafkaConnection.CACertificates,
		UserEnabledTls: util.BoolPtr(kafkaConnection.CACertificates != ""),
		Brokers:        kafkaConnection.ToBrokersUrls(),
		SubNetworkID:   connection.SubNetworkID,
		ConnectionID:   connection.ConnectionID,
	}

	return kafkaOptions
}

func ResolveKafkaAuth(auth *KafkaAuth, kafkaConnection *kafka.Connection) *KafkaAuth {
	if kafkaConnection == nil {
		return auth
	}

	resultMechanism := kafka.KafkaSaslSecurityMechanism_UNSPECIFIED
	for _, mechanism := range kafkaConnection.Mechanisms {
		if mechanism == kafka.KafkaSaslSecurityMechanism_SCRAM_SHA512 {
			resultMechanism = kafka.KafkaSaslSecurityMechanism_SCRAM_SHA512
			break
		} else if mechanism == kafka.KafkaSaslSecurityMechanism_SCRAM_SHA256 {
			resultMechanism = kafka.KafkaSaslSecurityMechanism_SCRAM_SHA256
		}
	}

	if resultMechanism == kafka.KafkaSaslSecurityMechanism_UNSPECIFIED {
		logger.Log.Infof("Resolved Kafka auth mechanism is UNSPECIFIED, using SCRAM-SHA-256")
		resultMechanism = kafka.KafkaSaslSecurityMechanism_SCRAM_SHA256
	}
	if resultMechanism == kafka.KafkaSaslSecurityMechanism_PLAIN {
		logger.Log.Infof("Resolved Kafka auth mechanism is PLAIN, using SCRAM-SHA-256")
		resultMechanism = kafka.KafkaSaslSecurityMechanism_SCRAM_SHA256
	}

	return &KafkaAuth{
		Enabled:   kafkaConnection.Password != "",
		Mechanism: resultMechanism,
		User:      kafkaConnection.User,
		Password:  string(kafkaConnection.Password),
	}
}

func resolveConnection(connectionID string) (*kafka.Connection, error) {
	connCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	//DP agent token here
	conn, err := connection.Resolver().ResolveConnection(connCtx, connectionID, ProviderType)
	if err != nil {
		return nil, err
	}

	if kafkaConn, ok := conn.(*kafka.Connection); ok {
		return kafkaConn, nil
	}

	return nil, xerrors.Errorf("Cannot cast connection %s to Kafka connection", connectionID)
}
