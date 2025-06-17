//go:build !disable_yt_provider

package statictable

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
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
		if err := initTable(client, opts); err != nil {
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
		scheme.Columns[i].Expression = ""
	}

	createOptions := createNodeOptions(scheme, opts.OptimizeFor, opts.CustomAttributes)
	createOptions.TransactionOptions = transactionOptions(opts.MainTxID)
	opts.Logger.Info("creating YT table with options", log.String("path",
		tmpTablePath.String()), log.Any("options", createOptions))

	if _, err := client.CreateNode(ctx, tmpTablePath, yt.NodeTable, &createOptions); err != nil {
		return xerrors.Errorf("unable to create static table on init stage: %w", err)
	}

	return nil
}
