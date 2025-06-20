//go:build !disable_mysql_provider

package mysql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/xtls"
)

func CreateCertPool(certPEMFile string, rootCAFiles []string) (*x509.CertPool, error) {
	if certPEMFile == "" {
		return xtls.Pool(rootCAFiles)
	} else {
		pool := x509.NewCertPool()
		if ok := pool.AppendCertsFromPEM([]byte(certPEMFile)); !ok {
			return nil, xerrors.New("Can't add cert PEM to pool")
		}
		return pool, nil
	}
}

func Connect(params *ConnectionParams, configAction func(config *mysql.Config) error) (*sql.DB, error) {
	config := mysql.NewConfig()

	// default settings
	config.Net = "tcp"

	// user settings
	config.Addr = fmt.Sprintf("%v:%v", params.Host, params.Port)
	config.User = params.User
	config.Passwd = params.Password
	config.DBName = params.Database
	config.Loc = params.Location

	if params.TLS {
		certPool, err := CreateCertPool(params.CertPEMFile, params.RootCAFiles)
		if err != nil {
			return nil, xerrors.Errorf("Can't configure TLS: %w", err)
		}
		tlsConfig := &tls.Config{
			InsecureSkipVerify: true,
			RootCAs:            certPool,
		}
		config.TLSConfig = "custom"
		if err := mysql.RegisterTLSConfig("custom", tlsConfig); err != nil {
			return nil, xerrors.Errorf("Can't configure TLS: %w", err)
		}
	}

	if configAction != nil {
		if err := configAction(config); err != nil {
			return nil, xerrors.Errorf("Config custom action error: %w", err)
		}
	}

	if config.AllowAllFiles {
		return nil, xerrors.New("Config parameter 'AllowAllFiles' is not allowed ")
	}

	connector, err := mysql.NewConnector(config)
	if err != nil {
		return nil, xerrors.Errorf("Can't create connector: %w", err)
	}

	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()

	db := sql.OpenDB(connector)
	rollbacks.AddCloser(db, logger.Log, "cannot close database")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return nil, xerrors.Errorf("Can't ping server: %w", err)
	}

	_, err = db.Exec("SET NAMES 'utf8mb4';")
	if err != nil {
		return nil, xerrors.Errorf("Can't set names: %w", err)
	}

	rollbacks.Cancel()
	return db, nil
}
