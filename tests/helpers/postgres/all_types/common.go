package postgres

import (
	"context"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferia/transferia/library/go/core/xerrors"
	postgres_canon "github.com/transferia/transferia/tests/canon/postgres"
)

const extensionsSQL = `
create extension if not exists hstore;
create extension if not exists ltree;
create extension if not exists citext;
`

// TableNames is the shared canon dataset all-types tests.
var TableNames = []string{
	"public.array_types",
	"public.date_types",
	"public.geom_types",
	"public.numeric_types",
	"public.text_types",
	"public.user_types",
	"public.wtf_types",
}

func EnsureExtensions(ctx context.Context, conn *pgxpool.Pool) error {
	if _, err := conn.Exec(ctx, extensionsSQL); err != nil {
		return xerrors.Errorf("ensure postgres extensions: %w", err)
	}
	return nil
}

func SeedTable(ctx context.Context, conn *pgxpool.Pool, tableName string) error {
	sql, ok := postgres_canon.TableSQLs[tableName]
	if !ok {
		return xerrors.Errorf("unknown canon table %s", tableName)
	}
	if _, err := conn.Exec(ctx, sql); err != nil {
		return xerrors.Errorf("seed canon table %s: %w", tableName, err)
	}
	return nil
}

func SeedAllTables(ctx context.Context, conn *pgxpool.Pool) error {
	if err := EnsureExtensions(ctx, conn); err != nil {
		return err
	}
	for _, tableName := range TableNames {
		if err := SeedTable(ctx, conn, tableName); err != nil {
			return err
		}
	}
	return nil
}
