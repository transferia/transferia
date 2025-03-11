package yt

import (
	"context"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/yt"
)

func TXOptions(txID yt.TxID) *yt.TransactionOptions {
	return &yt.TransactionOptions{TransactionID: txID}
}

func WithTx(ctx context.Context, client yt.Client, f func(ctx context.Context, tx yt.Tx) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tx, err := client.BeginTx(ctx, nil)
	if err != nil {
		return xerrors.Errorf("unable to begin transaction: %w", err)
	}

	err = f(ctx, tx)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return xerrors.Errorf("unable to commit transaction")
	}

	return nil
}
