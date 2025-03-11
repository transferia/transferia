package model

import (
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/dbaas"
)

func ResolvePassword(clusterID, user, password string) (string, error) {
	if clusterID == "" {
		return password, nil
	}

	if user != DefaultUser {
		return password, nil
	}
	provider, err := dbaas.Current()
	if err != nil {
		return "", xerrors.Errorf("unable to init clickhouse api for cluster %v: %w", clusterID, err)
	}
	resolver, err := provider.PasswordResolver(dbaas.ProviderTypeClickhouse, clusterID)
	if err != nil {
		if xerrors.Is(err, dbaas.NotSupported) {
			return password, nil
		}
		return "", xerrors.Errorf("unable to init password resolver: %w", err)
	}

	password, err = resolver.ResolvePassword()
	if err != nil {
		return "", xerrors.Errorf("cannot resolve clickhouse password: %w", err)
	}

	return password, nil
}
