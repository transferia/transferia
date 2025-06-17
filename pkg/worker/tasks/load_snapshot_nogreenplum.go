//go:build disable_postgres_provider

package tasks

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/greenplum"
)

func beginSnapshotGreenplum(ctx context.Context, l *SnapshotLoader, specificStorage *greenplum.Storage, tables []abstract.TableDescription) error {
	return xerrors.New("Greenplum is not available in this build")
}

func endSnapshotGreenplum(ctx context.Context, specificStorage *greenplum.Storage) error {
	return xerrors.New("Greenplum is not available in this build")
}

func endSnapshotPg(ctx context.Context) {}
