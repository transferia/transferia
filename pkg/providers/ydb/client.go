//go:build !disable_ydb_provider

package ydb

import (
	"context"
	"crypto/tls"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/ydb/logadapter"
	"github.com/transferia/transferia/pkg/xtls"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	ydbcreds "github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func newYDBDriver(
	ctx context.Context,
	database, instance string,
	credentials ydbcreds.Credentials,
	tlsConfig *tls.Config,
	verboseTraces bool,
) (*ydb.Driver, error) {
	secure := tlsConfig != nil

	traceLevel := trace.DriverEvents
	if verboseTraces {
		traceLevel = trace.DetailsAll
	}
	// TODO: it would be nice to handle some common errors such as unauthenticated one
	// but YDB driver error design makes this task extremely painful
	return ydb.Open(
		ctx,
		sugar.DSN(instance, database, sugar.WithSecure(secure)),
		ydb.WithCredentials(credentials),
		ydb.WithTLSConfig(tlsConfig),
		logadapter.WithTraces(logger.Log, traceLevel),
	)
}

func newYDBSourceDriver(ctx context.Context, cfg *YdbSource) (*ydb.Driver, error) {
	creds, err := ResolveCredentials(
		cfg.UserdataAuth,
		string(cfg.Token),
		JWTAuthParams{
			KeyContent:      cfg.SAKeyContent,
			TokenServiceURL: cfg.TokenServiceURL,
		},
		cfg.ServiceAccountID,
		cfg.OAuth2Config,
		logger.Log,
	)
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve creds: %w", err)
	}

	var tlsConfig *tls.Config
	if cfg.TLSEnabled {
		tlsConfig, err = xtls.FromPath(cfg.RootCAFiles)
		if err != nil {
			return nil, xerrors.Errorf("cannot create TLS config: %w", err)
		}
	}
	return newYDBDriver(ctx, cfg.Database, cfg.Instance, creds, tlsConfig, cfg.VerboseSDKLogs)
}
