//go:build !disable_postgres_provider

package postgres

import (
	"context"
	"strings"

	"github.com/jackc/pgtype/pgxtype"
	"github.com/transferia/transferia/internal/logger"
	"go.ytsaurus.tech/library/go/core/log"
)

type PgVersion struct {
	Is9x, Is10x, Is11x, Is12x, Is13x, Is14x bool
	Version                                 string
}

func NewPgVersion(version string) PgVersion {
	return PgVersion{
		Is9x:    strings.HasPrefix(version, "PostgreSQL 9."),
		Is10x:   strings.HasPrefix(version, "PostgreSQL 10."),
		Is11x:   strings.HasPrefix(version, "PostgreSQL 11."),
		Is12x:   strings.HasPrefix(version, "PostgreSQL 12."),
		Is13x:   strings.HasPrefix(version, "PostgreSQL 13."),
		Is14x:   strings.HasPrefix(version, "PostgreSQL 14."),
		Version: version,
	}
}

func ResolveVersion(pool pgxtype.Querier) PgVersion {
	version := "unknown"
	if err := pool.QueryRow(context.TODO(), "SELECT version()").Scan(&version); err != nil {
		logger.Log.Error("failed to resolve PostgreSQL version", log.Error(err))
	}
	return NewPgVersion(version)
}
