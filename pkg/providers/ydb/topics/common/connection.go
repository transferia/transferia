package topiccommon

import (
	"context"
	"fmt"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	ydbprovider "github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/pkg/providers/ydb/logadapter"
	"github.com/transferia/transferia/pkg/xtls"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.ytsaurus.tech/library/go/core/log"
)

const defaultPort = 2135

type ConnectionConfig struct {
	Endpoint    string
	Database    string
	Credentials ydbprovider.TokenCredentials

	TLSEnabled       bool
	RootCAFiles      []string
	TLSCACertificate string
}

// FormatEndpoint returns an instance if it has a port, or joins the instance with a port.
// If the port is 0, default port will be used.
func FormatEndpoint(instance string, port int) string {
	if instanceContainsPort(instance) {
		return instance
	}

	endpointInstance, endpointPort := instance, defaultPort
	if port != 0 {
		endpointPort = port
	}

	return fmt.Sprintf("%s:%d", endpointInstance, endpointPort)
}

func instanceContainsPort(instance string) bool {
	for i := range instance {
		if instance[i] == ':' {
			return true
		}
	}
	return false
}

func NewYDBDriver(cfg ConnectionConfig, lgr log.Logger) (*ydb.Driver, error) {
	isSecure := false
	opts := []ydb.Option{
		logadapter.WithTraces(lgr, trace.DetailsAll),
		ydb.With(
			config.WithOperationTimeout(60 * time.Second), // to prevent some hanging-on
		),
	}

	if cfg.Credentials != nil {
		opts = append(opts, ydb.WithCredentials(cfg.Credentials))
	}

	if cfg.TLSCACertificate != "" {
		isSecure = true
		tlsConfig, err := xtls.FromContent(cfg.TLSCACertificate)
		if err != nil {
			return nil, xerrors.Errorf("could not create TLS config from certificate content: %w", err)
		}
		opts = append(opts, ydb.WithTLSConfig(tlsConfig))
	} else if cfg.TLSEnabled {
		isSecure = true
		tlsConfig, err := xtls.FromPath(cfg.RootCAFiles)
		if err != nil {
			return nil, xerrors.Errorf("cannot init driver without tls: %w", err)
		}
		opts = append(opts, ydb.WithTLSConfig(tlsConfig))
	}

	driverCtx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	return ydb.Open(driverCtx, sugar.DSN(cfg.Endpoint, cfg.Database, sugar.WithSecure(isSecure)), opts...)
}
