package ydb

import (
	"context"
	"crypto/tls"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/credentials"
	"github.com/transferia/transferia/pkg/providers/ydb/logadapter"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	ydbcreds "github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"github.com/ydb-platform/ydb-go-yc-metadata"
)

func newYDBDriver(ctx context.Context, database, instance string, credentials ydbcreds.Credentials, tlsConfig *tls.Config) (*ydb.Driver, error) {
	secure := tlsConfig != nil
	driver, err := ydb.Open(
		ctx,
		sugar.DSN(instance, database, sugar.WithSecure(secure)),
		ydb.WithCredentials(credentials),
		ydb.WithTLSConfig(tlsConfig),
		logadapter.WithTraces(logger.Log, trace.DriverEvents),
	)
	if err != nil {
		return nil, xerrors.Errorf("YDB open error: %w", err)
	}

	return driver, nil
}

func newYDBSourceDriver(ctx context.Context, cfg *YdbSource) (*ydb.Driver, error) {
	dsn, opts, err := ydbSourceToCreds(cfg)
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve creds: %w", err)
	}

	if cfg.VerboseSDKLogs {
		opts = append(opts, logadapter.WithTraces(logger.Log, trace.DetailsAll))
	}

	db, err := ydb.Open(ctx, dsn, opts...)
	if err != nil {
		return nil, xerrors.Errorf("unable to create ydb client, err: %w", err)
	}

	return db, nil
}

func ydbSourceToCreds(cfg *YdbSource) (string, []ydb.Option, error) {
	if cfg.SAKeyContent != "" {
		return "", nil, abstract.NewFatalError(xerrors.New("SAKeyContent is not supported for now"))
	}

	isSecure := false
	opts := make([]ydb.Option, 0)

	if cfg.TLSEnabled {
		isSecure = true
		opts = append(opts, yc.WithInternalCA())
	}
	if cfg.ServiceAccountID != "" {
		creds, err := credentials.NewServiceAccountCreds(logger.Log, cfg.ServiceAccountID)
		if err != nil {
			return "", nil, xerrors.Errorf("could not create service account YDB credentials: %w", err)
		}
		opts = append(opts, ydb.WithCredentials(creds))
	} else {
		opts = append(opts, ydb.WithAccessTokenCredentials(string(cfg.Token)))
	}

	return sugar.DSN(cfg.Instance, cfg.Database, sugar.WithSecure(isSecure)), opts, nil
}
