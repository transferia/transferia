//go:build !disable_yt_provider

package statictable

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type CommitOptions struct {
	MainTxID         yt.TxID
	TransferID       string
	Schema           []abstract.ColSchema
	Path             ypath.Path
	CleanupType      model.CleanupType
	AllowedSorting   bool
	Pool             string
	OptimizeFor      string
	CustomAttributes map[string]any
	Logger           log.Logger
	IsDynamicSorted  bool
	ReduceBinaryPath ypath.Path
}

func Commit(client yt.Client, opts *CommitOptions) error {
	return backoff.Retry(func() error {
		if err := commit(client, opts); err != nil {
			opts.Logger.Warn("committing table error",
				log.String("table_path", opts.Path.String()), log.Error(err))
			return xerrors.Errorf("cannot commit static table: %w", err)
		}
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), retriesCount))
}

func commit(client yt.Client, opts *CommitOptions) error {
	commitFn := func(tx yt.Tx) error {
		currentStageTablePath := makeTablePath(opts.Path, opts.TransferID, tmpNamePostfix)
		sortedTablePath := makeTablePath(opts.Path, opts.TransferID, sortedNamePostfix)

		commitCl := newCommitClient(tx, client, opts.Schema, opts.Pool, opts.OptimizeFor, opts.CustomAttributes, opts.IsDynamicSorted)

		var err error
		var startMoment time.Time
		if opts.AllowedSorting {
			startMoment = time.Now()
			currentStageTablePath, err = commitCl.sortTable(currentStageTablePath, sortedTablePath)
			if err != nil {
				return xerrors.Errorf("sorting static table error: %w", err)
			}
			opts.Logger.Info("successfully completed commit step: sorting static table",
				log.Any("table_path", opts.Path), log.Duration("elapsed_time", time.Since(startMoment)))
		}

		if opts.CleanupType != model.Drop {
			startMoment = time.Now()
			sortedMerge := currentStageTablePath == sortedTablePath
			if opts.IsDynamicSorted {
				reducedTablePath := makeTablePath(opts.Path, opts.TransferID, reducedNamePostfix)
				currentStageTablePath, err = commitCl.reduceTables(currentStageTablePath, opts.Path, reducedTablePath, opts.ReduceBinaryPath) // add actual binary path
				if err != nil {
					return xerrors.Errorf("reducing static table error: %w", err)
				}
			} else {
				if err := commitCl.mergeTables(currentStageTablePath, opts.Path, sortedMerge); err != nil {
					return xerrors.Errorf("merging static table error: %w", err)
				}
			}
			opts.Logger.Info("successfully completed commit step: static table merging",
				log.Any("table_path", opts.Path), log.Duration("elapsed_time", time.Since(startMoment)))
		}

		if err := commitCl.moveTables(currentStageTablePath, opts.Path); err != nil {
			return xerrors.Errorf("merging static table error: %w", err)
		}

		return nil
	}

	return execInSubTx(client, opts.MainTxID, opts.Logger, commitFn)
}

func execInSubTx(client yt.Client, parentTxID yt.TxID, logger log.Logger, fn func(tx yt.Tx) error) error {
	abortTx := util.Rollbacks{}
	defer abortTx.Do()

	ctx := context.Background()
	tx, err := client.BeginTx(ctx, &yt.StartTxOptions{
		Timeout:            &subTxTimeout,
		TransactionOptions: transactionOptions(parentTxID),
	})
	if err != nil {
		return xerrors.Errorf("beginning sub transaction error: %w", err)
	}
	abortTx.Add(func() {
		if err := tx.Abort(); err != nil {
			logger.Error("cannot abort static table sub transaction", log.Any("tx", tx.ID()), log.Error(err))
		}
	})

	if err := fn(tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return xerrors.Errorf("commit sub transaction error: %w", err)
	}
	abortTx.Cancel()

	return nil
}
