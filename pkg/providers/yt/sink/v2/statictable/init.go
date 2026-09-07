package statictable

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/errors/codes"
	yt2 "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type InitOptions struct {
	MainTxID         yt.TxID
	TransferID       string
	Schema           []abstract.ColSchema
	Path             ypath.Path
	OptimizeFor      string
	CustomAttributes map[string]any
	Logger           log.Logger
}

func Init(client yt.Client, opts *InitOptions) error {
	return backoff.Retry(func() error {
		err := initTable(client, opts)
		if err != nil && (codes.YTAccountLimitExceeded.Contains(err) || codes.YTAccessDenied.Contains(err)) {
			return backoff.Permanent(xerrors.Errorf("unable to init static table writing: %w", err))
		}
		if err != nil {
			return xerrors.Errorf("unable to init static table writing: %w", err)
		}
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), retriesCount))
}

func initTable(client yt.Client, opts *InitOptions) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tmpTablePath := makeTablePath(opts.Path, opts.TransferID, tmpNamePostfix)
	scheme := makeYtSchema(opts.Schema)
	for i := range scheme.Columns {
		scheme.Columns[i].SortOrder = ""
	}

	createOptions := createNodeOptions(scheme, opts.OptimizeFor, opts.CustomAttributes)
	opts.Logger.Info("creating YT table with options", log.String("path",
		tmpTablePath.String()), log.Any("options", createOptions))

	// a child tx rolls the node back if the call times out on the client after succeeding on the server
	tx, err := client.BeginTx(ctx, &yt.StartTxOptions{TransactionOptions: transactionOptions(opts.MainTxID)})
	if err != nil {
		return xerrors.Errorf("unable to begin init transaction: %w", yt2.WrapYTError(err))
	}
	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()
	rollbacks.Add(func() {
		if err := tx.Abort(); err != nil {
			opts.Logger.Warn("unable to abort init transaction", log.Error(err))
		}
	})

	if _, err := tx.CreateNode(ctx, tmpTablePath, yt.NodeTable, &createOptions); err != nil {
		return xerrors.Errorf("unable to create static table on init stage: %w", yt2.WrapYTError(err))
	}
	rollbacks.Cancel() // after a failed Commit the tx is aborted by the yt client itself
	if err := tx.Commit(); err != nil {
		return xerrors.Errorf("unable to commit init transaction: %w", yt2.WrapYTError(err))
	}
	return nil
}
