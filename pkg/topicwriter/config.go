package topicwriter

import (
	"crypto/tls"
	"fmt"

	"github.com/transferia/transferia/internal/staticcreds"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

const (
	logbrokerDefaultPort     = 2135
	logbrokerDefaultDatabase = "/Root"

	maxWriterQueueLen = 10_000
)

type Config struct {
	Instance    string
	Port        int
	Database    string
	Token       string
	Topic       string
	SourceID    string
	Credentials staticcreds.TokenCredentials
	TlsConfig   *tls.Config
	Codec       topictypes.Codec

	// UseFederation turns on a federated writer for the logbroker.yandex.net installation.
	// It can only work with Logbroker.
	UseFederation bool

	// By default (false), Write() blocks until queue space is available or ctx is cancelled.
	WithWriterErrOnQueueFull bool
}

func (c Config) Endpoint() string {
	port := c.Port
	if port == 0 {
		port = logbrokerDefaultPort
	}
	return fmt.Sprintf("%s:%d", c.Instance, port)
}

func (c Config) DB() string {
	if c.Database == "" {
		return logbrokerDefaultDatabase
	}
	return c.Database
}

func (c Config) Creds() staticcreds.TokenCredentials {
	if c.Credentials != nil {
		return c.Credentials
	}

	if c.Token != "" {
		return staticcreds.New(c.Token)
	}

	return nil
}
