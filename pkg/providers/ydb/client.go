package ydb

import (
	"context"
	"crypto/tls"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/errors/coded"
	error_codes "github.com/transferia/transferia/pkg/errors/codes"
	"github.com/transferia/transferia/pkg/providers/ydb/logadapter"
	"github.com/transferia/transferia/pkg/xtls"
	ydb_go_sdk "github.com/ydb-platform/ydb-go-sdk/v3"
	ydb_credentials "github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	grpc_codes "google.golang.org/grpc/codes"
	grpc_status "google.golang.org/grpc/status"
)

func newYDBDriver(
	ctx context.Context,
	database, instance string,
	credentials ydb_credentials.Credentials,
	tlsConfig *tls.Config,
) (*ydb_go_sdk.Driver, error) {
	secure := tlsConfig != nil

	// TODO: it would be nice to handle some common errors such as unauthenticated one
	// but YDB driver error design makes this task extremely painful
	d, err := ydb_go_sdk.Open(
		ctx,
		sugar.DSN(instance, database, sugar.WithSecure(secure)),
		ydb_go_sdk.WithCredentials(credentials),
		ydb_go_sdk.WithTLSConfig(tlsConfig),
		logadapter.WithTraces(logger.Log, trace.DetailsAll),
	)
	if err != nil {
		if s, ok := grpc_status.FromError(err); ok && s.Code() == grpc_codes.NotFound {
			return nil, coded.Errorf(error_codes.YDBNotFound, "Cannot create YDB driver: %w", err)
		}
		return nil, xerrors.Errorf("Cannot create YDB driver: %w", err)
	}
	return d, nil
}

func newYDBSourceDriver(ctx context.Context, cfg *YdbSource) (*ydb_go_sdk.Driver, error) {
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
	if cfg.TLSCACertificate != "" {
		tlsConfig, err = xtls.FromContent(cfg.TLSCACertificate)
		if err != nil {
			return nil, xerrors.Errorf("cannot create TLS config from certificate content: %w", err)
		}
	} else if cfg.TLSEnabled {
		tlsConfig, err = xtls.FromPath(cfg.RootCAFiles)
		if err != nil {
			return nil, xerrors.Errorf("cannot create TLS config: %w", err)
		}
	}
	return newYDBDriver(ctx, cfg.Database, cfg.Instance, creds, tlsConfig)
}
