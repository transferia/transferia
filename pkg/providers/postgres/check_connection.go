package postgres

import (
	"context"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

// checkConnection connects once with a bare config: no data type initialization, coded errors as in NewPgConnPoolConfig
func checkConnection(ctx context.Context, connConfig *pgx.ConnConfig, err error) error {
	if err != nil {
		return xerrors.Errorf("connection config: %w", err)
	}
	poolConfig, _ := pgxpool.ParseConfig("")
	poolConfig.ConnConfig = connConfig
	poolConfig.LazyConnect = true
	pool, err := NewPgConnPoolConfig(ctx, poolConfig)
	if err != nil {
		return err
	}
	pool.Close()
	return nil
}
