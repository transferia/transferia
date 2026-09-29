package mysql

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

func checkConnection(ctx context.Context, storage *MysqlStorageParams) error {
	connParams, err := NewConnectionParams(storage)
	if err != nil {
		return xerrors.Errorf("connection params: %w", err)
	}
	db, err := ConnectContext(ctx, connParams, nil)
	if err != nil {
		return xerrors.Errorf("connect: %w", err)
	}
	_ = db.Close()
	return nil
}
