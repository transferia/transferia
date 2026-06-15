package kafka

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/twmb/franz-go/pkg/kgo"
)

// franzDialTLS wraps a custom dial (e.g. MDB host replacement) with a TLS client handshake.
// kgo forbids setting Dialer and DialTLSConfig together; this combines them in one Dialer.
func franzDialTLS(
	dial func(context.Context, string, string) (net.Conn, error),
	tlsCfg *tls.Config,
) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		raw, err := dial(ctx, network, addr)
		if err != nil {
			return nil, xerrors.Errorf("unable to dial to %s: %w", addr, err)
		}
		cfg := tlsCfg.Clone()
		if cfg.ServerName == "" {
			if host, _, splitErr := net.SplitHostPort(addr); splitErr == nil {
				cfg.ServerName = host
			}
		}
		conn := tls.Client(raw, cfg)
		if err := conn.HandshakeContext(ctx); err != nil {
			_ = raw.Close()
			return nil, xerrors.Errorf("unable to handshake with %s: %w", addr, err)
		}
		return conn, nil
	}
}

// franz-go: cannot set both kgo.Dialer and kgo.DialTLSConfig — combine when DialFunc is set.
func kgoDialTLSConfig(cfg *KafkaSource, tlsConfig *tls.Config) kgo.Opt {
	switch {
	case cfg.DialFunc != nil && tlsConfig != nil:
		return kgo.Dialer(franzDialTLS(cfg.DialFunc, tlsConfig))
	case cfg.DialFunc != nil:
		return kgo.Dialer(cfg.DialFunc)
	default:
		return kgo.DialTLSConfig(tlsConfig)
	}
}

func kafkaClientCommonOptions(cfg *KafkaSource) ([]kgo.Opt, error) {
	tlsConfig, err := cfg.Connection.TLSConfig()
	if err != nil {
		return nil, xerrors.Errorf("unable to get TLS config: %w", err)
	}
	cfg.Auth.Password, err = ResolvePassword(cfg.Connection, cfg.Auth)
	if err != nil {
		return nil, xerrors.Errorf("unable to get password: %w", err)
	}
	mechanism := cfg.Auth.GetFranzAuthMechanism()
	brokers, err := ResolveBrokers(cfg.Connection)
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve brokers: %w", err)
	}

	// common kafka client options
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgoDialTLSConfig(cfg, tlsConfig),
		kgo.FetchMaxBytes(10 * 1024 * 1024), // 10MB
		kgo.ConnIdleTimeout(30 * time.Second),
		kgo.RequestTimeoutOverhead(20 * time.Second),
	}

	if mechanism != nil {
		opts = append(opts, kgo.SASL(mechanism))
	}

	return opts, nil
}
