//go:build disable_postgres_provider

package tasks

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/postgres"
)

func beginSnapshotPg(ctx context.Context, l *SnapshotLoader, specificStorage *postgres.Storage) error {
	return xerrors.New("Postgres is not available in this build")
}
