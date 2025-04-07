package kafka

import (
	"context"
	"net"

	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/dbaas"
	"github.com/transferia/transferia/pkg/providers/kafka/client"
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
