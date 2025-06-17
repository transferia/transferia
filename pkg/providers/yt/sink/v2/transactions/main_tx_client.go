//go:build !disable_yt_provider

package transactions

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

const (
	maxRetriesCount uint64 = 5
)

var (
	mainTxTimeout = yson.Duration(time.Minute * 20)
	partTxTimeout = yson.Duration(time.Minute * 10)
)

type txStateStorage interface {
	GetState() (*yt.TxID, error)
	SetState(tx yt.TxID) error
	RemoveState() (*yt.TxID, error)
}

type MainTxClient struct {
	client       yt.Client
	id           *yt.TxID
	stateStorage txStateStorage

	cancelPinger func()

	logger log.Logger
}

// BeginTx starts a new main transaction and saves it to the transfer state.
// Also, previous saved transaction will be aborted and removed from state.
func (c *MainTxClient) BeginTx() error {
	if c.id != nil {
		return nil
	}

	prevMainTxID, err := c.stateStorage.RemoveState()
	if err != nil {
		return xerrors.Errorf("cannot remove state: %w", err)
	}
	if prevMainTxID != nil {
		c.logger.Info("remove and abort previous tx from state", log.String("previous_main_tx", prevMainTxID.String()))
		if err := c.client.AbortTx(context.Background(), *prevMainTxID, nil); err != nil {
			c.logger.Error("cannot abort previous main transaction", log.String("tx_id", prevMainTxID.String()), log.Error(err))
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	txID, err := c.client.StartTx(ctx, &yt.StartTxOptions{
		Timeout: &mainTxTimeout,
	})
	if err != nil {
		c.logger.Error("cannot start sink main tx for snapshot", log.Error(err))
		return err
	}

	if err := c.stateStorage.SetState(txID); err != nil {
		return xerrors.Errorf("cannot set mainTxID to state: %w", err)
	}

	c.id = &txID
	c.logger.Info("yt main tx has been started", log.Any("tx_id", txID))

	return nil
}

// ExecOrAbort performs a function using the main transaction.
// In case of an error, aborts the main transaction and closes the client.
func (c *MainTxClient) ExecOrAbort(fn func(mainTxID yt.TxID) error) error {
	if err := c.checkClientCondition(); err != nil {
		return xerrors.Errorf("using main transaction error: %w", err)
	}

	abort := util.Rollbacks{}
	defer abort.Do()
	abort.Add(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := c.client.AbortTx(ctx, *c.id, nil); err != nil {
			c.logger.Error("cannot abort main transaction", log.String("tx_id", c.id.String()), log.Error(err))
		}
		c.logger.Error("main transaction was aborted", log.String("tx_id", c.id.String()))
		c.Close()
	})

	if err := fn(*c.id); err != nil {
		return err
	}
	abort.Cancel()

	return nil
}

// BeginSubTx creates a child transaction from the main transaction.
func (c *MainTxClient) BeginSubTx() (yt.Tx, error) {
	if err := c.checkClientCondition(); err != nil {
		return nil, xerrors.Errorf("begin sub transaction error: %w", err)
	}

	partTx, err := c.client.BeginTx(context.Background(), &yt.StartTxOptions{
		Timeout: &partTxTimeout,
		TransactionOptions: &yt.TransactionOptions{
			TransactionID: *c.id,
			Ping:          true,
			PingAncestors: true,
		},
	})
	if err != nil {
		return nil, xerrors.Errorf("unable to begin part transaction: %w", err)
	}
	c.logger.Info("part transaction has been started", log.Any("tx_id", partTx.ID()))
	return partTx, nil
}

// Commit commits the main transaction or abort it if an error occurs.
// Anyway, the client will be closed.
func (c *MainTxClient) Commit() error {
	defer c.Close()

	fn := func(mainTxID yt.TxID) error {
		if err := backoff.Retry(func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if err := c.client.CommitTx(ctx, mainTxID, nil); err != nil {
				return xerrors.Errorf("cannot commit main transaction: %w", err)
			}
			return nil
		}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), maxRetriesCount)); err != nil {
			c.logger.Error("error commit sink, retries didnt help", log.Error(err))
			return err
		}

		return nil
	}

	return c.ExecOrAbort(fn)
}

func (c *MainTxClient) Close() {
	if c.cancelPinger != nil {
		c.cancelPinger()
		c.cancelPinger = nil
	}
}

func (c *MainTxClient) checkClientCondition() error {
	if err := c.checkTx(); err != nil {
		return err
	}
	if c.cancelPinger == nil {
		c.cancelPinger = beginTransactionPinger(c.client, *c.id, c.logger)
	}
	return nil
}

func (c *MainTxClient) checkTx() error {
	if c.id != nil {
		return nil
	}

	txID, err := c.stateStorage.GetState()
	if err != nil {
		return xerrors.Errorf("unable to get mainTxID from state: %w", err)
	}
	c.id = txID
	return nil
}

func NewMainTxClient(transferID string, cp coordinator.Coordinator, client yt.Client, logger log.Logger) *MainTxClient {
	return &MainTxClient{
		client:       client,
		id:           nil,
		stateStorage: newYtStateStorage(cp, transferID, logger),
		cancelPinger: nil,
		logger:       logger,
	}
}
