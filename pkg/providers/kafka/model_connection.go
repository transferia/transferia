//go:build !disable_kafka_provider

package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"net"

	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/util/validators"
	franzsasl "github.com/twmb/franz-go/pkg/sasl"
	franzscram "github.com/twmb/franz-go/pkg/sasl/scram"
)

type KafkaAuth struct {
	Enabled   bool
	Mechanism string
	User      string
	Password  string
}

func (a *KafkaAuth) GetAuthMechanism() (sasl.Mechanism, error) {
	if !a.Enabled {
		return nil, nil
	}
	var algo scram.Algorithm
	if a.Mechanism == "SHA-512" {
		algo = scram.SHA512
	} else {
		algo = scram.SHA256
	}
	m, err := scram.Mechanism(algo, a.User, a.Password)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (a *KafkaAuth) GetFranzAuthMechanism() franzsasl.Mechanism {
	if !a.Enabled {
		return nil
	}
	auth := franzscram.Auth{
		User: a.User,
		Pass: a.Password,
	}
	if a.Mechanism == "SHA-512" {
		return auth.AsSha512Mechanism()
	}

	return auth.AsSha256Mechanism()
}

type KafkaConnectionOptions struct {
	ClusterID    string
	TLS          model.TLSMode
	TLSFile      string `model:"PemFileContent"`
	Brokers      []string
	SubNetworkID string
}

func (o *KafkaConnectionOptions) TLSConfig() (*tls.Config, error) {
	if o.TLS == model.DisabledTLS {
		return nil, nil
	}
	if o.TLSFile != "" {
		cp := x509.NewCertPool()
		if !cp.AppendCertsFromPEM([]byte(o.TLSFile)) {
			return nil, xerrors.Errorf("credentials: failed to append certificates")
		}
		return &tls.Config{
			RootCAs: cp,
		}, nil
	}
	return &tls.Config{
		InsecureSkipVerify: true,
	}, nil
}

// BrokersHostnames returns a list of brokers' hostnames
func (o *KafkaConnectionOptions) BrokersHostnames() ([]string, error) {
	result := make([]string, len(o.Brokers))
	for i, b := range o.Brokers {
		if host, _, err := net.SplitHostPort(b); err != nil {
			return nil, xerrors.Errorf("failed to split Kafka broker URL %q to host and port: %w", b, err)
		} else {
			if err := validators.Host(host); err != nil {
				return nil, xerrors.Errorf("failed to validate Kafka broker host: %w", err)
			}
			result[i] = host
		}
	}
	return result, nil
}
