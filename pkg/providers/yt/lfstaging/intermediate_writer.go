//go:build !disable_yt_provider

package lfstaging

import (
	"context"
	"sync"
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/xerrors"
)

const (
	rotatorDelay = time.Millisecond * 1000
)

type intermediateWriter struct {
	config   *sinkConfig
	logger   log.Logger
	ytClient yt.Client

	lockingTx yt.Tx
	tablePath ypath.Path

	writerCreatedAt time.Time
	writtenBytes    int64

	lock sync.Mutex
}

const (
	writerLockAttr = "_lfstaging_writer_lock"
)

func newIntermediateWriter(config *sinkConfig, ytClient yt.Client, logger log.Logger) (*intermediateWriter, error) {
	iw := &intermediateWriter{
		config:          config,
		logger:          logger,
		ytClient:        ytClient,
		lockingTx:       nil,
		tablePath:       "",
		writerCreatedAt: time.Time{},
		writtenBytes:    0,
		lock:            sync.Mutex{},
	}

	if err := iw.rotate(); err != nil {
		return nil, xerrors.Errorf("cannot do initial table rotation: %w", err)
	}

	iw.startRotatorFiber()

	return iw, nil
}

func (iw *intermediateWriter) Write(items []abstract.ChangeItem) error {
	iw.lock.Lock()
	defer iw.lock.Unlock()

	iw.logger.Infof("intermediate writer: writing %v items into intermediate table", len(items))

	return yt.ExecTx(context.TODO(), iw.ytClient, func(ctx context.Context, tx yt.Tx) error {
		metadata, err := lbMetaFromTableAttr(tx, iw.tablePath)
		if err != nil {
			return xerrors.Errorf("cannot request @_logbroker_metadata: %w", err)
		}

		writer, err := tx.WriteTable(
			ctx,
			ypath.NewRich(iw.tablePath.String()).SetAppend(),
			nil,
		)
		if err != nil {
			return xerrors.Errorf("cannot create table writer: %w", err)
		}
		defer writer.Commit()

		for _, item := range items {
			row, err := intermediateRowFromChangeItem(item)
			if err != nil {
				return xerrors.Errorf("cannot convert changeitem to intermediate row: %w", err)
			}

			iw.writtenBytes += int64(len(row.Data))

			if iw.config.useNewMetadataFlow {
				outputRow := lfStagingRowFromIntermediate(row)
				if err = writer.Write(outputRow); err != nil {
					return xerrors.Errorf("cannot write into the logfeller writer: %w", err)
				}
			} else {
				if err = writer.Write(row); err != nil {
					return xerrors.Errorf("cannot write into the intermediate writer: %w", err)
				}
			}

			metadata.AddIntermediateRow(row)
		}

		if err := metadata.saveIntoTableAttr(tx, iw.tablePath); err != nil {
			return xerrors.Errorf("cannot save @_logbroker_metadata: %w", err)
		}

		return nil
	}, nil)
}

func (iw *intermediateWriter) startRotatorFiber() {
	iw.logger.Info("intermediate writer: starting rotator fiber")
	go func() {
		for {
			if !iw.needsRotating() {
				iw.logger.Infof("intermediate writer: waiting (created=%v, size=%v)", iw.writerCreatedAt, iw.writtenBytes)
				time.Sleep(rotatorDelay)
				continue
			}

			iw.logger.Infof("intermediate writer: rotating current table %v (created=%v, size=%v)", iw.tablePath, iw.writerCreatedAt, iw.writtenBytes)

			if err := iw.rotate(); err != nil {
				iw.logger.Errorf("intermediate writer: could not rotate: %v", err)
			}
		}
	}()
}

func (iw *intermediateWriter) needsRotating() bool {
	timeSinceCreated := time.Since(iw.writerCreatedAt)
	shouldRotateOnTime := int64(timeSinceCreated/time.Second) > iw.config.secondsPerTmpTable
	shouldRotateOnSize := iw.writtenBytes > iw.config.bytesPerTmpTable

	return (shouldRotateOnTime || shouldRotateOnSize) && iw.writtenBytes > 0
}

func (iw *intermediateWriter) rotate() error {
	iw.lock.Lock()
	defer iw.lock.Unlock()

	if iw.lockingTx != nil {
		if err := iw.lockingTx.Commit(); err != nil {
			return xerrors.Errorf("cannot commit previous locking tx: %w", err)
		}
		iw.lockingTx = nil
	}

	tableName := guid.New().String()
	iw.tablePath = iw.config.tmpPath.Child(tableName)

	_, err := yt.CreateTable(
		context.TODO(),
		iw.ytClient,
		iw.tablePath,
		yt.WithRecursive(),
		yt.WithAttributes(map[string]any{
			"_logbroker_metadata": newLogbrokerMetadata().serialize(),
		}),
	)
	if err != nil {
		return xerrors.Errorf("cannot create new table: %w", err)
	}

	lockingTxTimeout := yson.Duration(time.Second * time.Duration(iw.config.secondsPerTmpTable) * 2)
	iw.lockingTx, err = iw.ytClient.BeginTx(context.TODO(), &yt.StartTxOptions{
		Timeout: &lockingTxTimeout,
	})
	if err != nil {
		return xerrors.Errorf("cannot start locking tx: %w", err)
	}

	// wtf? why does yt require ptrs in options?
	writerLockAttrCopy := writerLockAttr
	lockOpts := &yt.LockNodeOptions{
		AttributeKey: &writerLockAttrCopy,
	}
	if _, err := iw.lockingTx.LockNode(context.TODO(), iw.tablePath, yt.LockShared, lockOpts); err != nil {
		return xerrors.Errorf("cannot lock under locking tx: %w", err)
	}

	err = iw.lockingTx.SetNode(
		context.TODO(),
		iw.tablePath.Child("@"+writerLockAttr),
		1,
		nil,
	)
	if err != nil {
		return xerrors.Errorf("cannot set writer lock under locking tx: %w", err)
	}

	iw.writtenBytes = 0
	iw.writerCreatedAt = time.Now()

	return nil
}
