package clickhouse

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/model"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	transformer_dbt "github.com/transferia/transferia/pkg/transformer/registry/dbt"
)

func init() {
	transformer_dbt.Register(New)
}

type Adapter struct {
	*clickhouse_model.ChDestination
}

func (d *Adapter) DBTConfiguration(ctx context.Context) (any, error) {
	storageParams, err := d.ToStorageParams()
	if err != nil {
		return nil, xerrors.Errorf("failed to resolve storage params: %w", err)
	}
	hosts, err := clickhouse_model.ConnectionHosts(ctx, storageParams, "")
	if err != nil {
		return nil, xerrors.Errorf("failed to obtain a list of hosts for the destination ClickHouse: %w", err)
	}
	if len(hosts) == 0 {
		return nil, xerrors.New("hosts is required")
	}
	host := hosts[0]
	if host.Name == "localhost" {
		host.Name = "host.docker.internal" // DBT runs inside docker, so localhost there is a host.docker.internal
	}

	return map[string]any{
		"type":     "clickhouse",
		"schema":   d.Database,
		"host":     host.Name,
		"port":     host.HTTPPort,
		"user":     d.User,
		"password": string(d.Password),
		"secure":   d.SSLEnabled || d.MdbClusterID != "",
	}, nil
}

func New(endpoint model.Destination) (transformer_dbt.SupportedDestination, error) {
	ch, ok := endpoint.(*clickhouse_model.ChDestination)
	if !ok {
		return nil, transformer_dbt.NotSupportedErr
	}
	return &Adapter{ch}, nil
}
